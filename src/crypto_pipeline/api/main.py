"""API REST de service des données (FastAPI).

Conçue pour le BACKTESTING : séries OHLCV paginées, multi-intervalles, rapides
(pool de connexions + index sur la clé primaire (symbol, open_time)).
Documentation interactive : http://localhost:8000/docs
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from crypto_pipeline import __version__
from crypto_pipeline.api import schemas
from crypto_pipeline.config import get_settings
from crypto_pipeline.logging_conf import get_logger, kv

log = get_logger("crypto_pipeline.api")

Interval = Literal["1m", "5m", "15m", "1h", "1d"]
# Noms de tables fixés côté serveur : jamais construits à partir de l'entrée utilisateur.
_STEP = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "15m": timedelta(minutes=15),
         "1h": timedelta(hours=1), "1d": timedelta(days=1)}
MAX_LIMIT = 10_000


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ThreadedConnectionPool(1, 10, get_settings().database_url)
    log.info(kv(event="api_started", version=__version__))
    yield
    app.state.pool.closeall()


app = FastAPI(
    title="Crypto Trading Data API",
    version=__version__,
    description="Données crypto (Binance + CoinGecko) servies depuis PostgreSQL pour le backtesting.",
    lifespan=lifespan,
)


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    """Mesure la latence de chaque requête (en-tête X-Process-Time-ms + log)."""
    t0 = time.perf_counter()
    response = await call_next(request)
    ms = (time.perf_counter() - t0) * 1000
    response.headers["X-Process-Time-ms"] = f"{ms:.1f}"
    log.info(kv(event="request", method=request.method, path=request.url.path,
                status=response.status_code, ms=f"{ms:.1f}"))
    return response


@contextmanager
def _cursor(request: Request) -> Iterator[RealDictCursor]:
    pool: ThreadedConnectionPool = request.app.state.pool
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _require_symbol(cur, symbol: str) -> str:
    symbol = symbol.upper()
    cur.execute("SELECT 1 FROM crypto.assets WHERE symbol = %s", (symbol,))
    if cur.fetchone() is None:
        raise HTTPException(404, f"Crypto inconnue : {symbol}")
    return symbol


@app.get("/health", tags=["monitoring"])
def health(request: Request, response: Response):
    """Santé de l'API et de la base (utilisé par le healthcheck Docker)."""
    try:
        with _cursor(request) as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "version": __version__}
    except Exception as exc:  # noqa: BLE001
        response.status_code = 503
        return {"status": "degraded", "error": str(exc)[:100]}


@app.get("/assets", response_model=list[schemas.Asset], tags=["référentiel"])
def list_assets(request: Request):
    with _cursor(request) as cur:
        cur.execute("SELECT symbol, name, binance_symbol, coingecko_id, active "
                    "FROM crypto.assets ORDER BY symbol")
        return cur.fetchall()


@app.get("/klines/{symbol}", response_model=schemas.KlinesResponse, tags=["prix"])
def get_klines(
    request: Request,
    symbol: str,
    interval: Interval = "1m",
    start: datetime | None = Query(None, description="Début inclusif (ISO 8601, UTC)"),
    end: datetime | None = Query(None, description="Fin exclusive (ISO 8601, UTC)"),
    limit: int = Query(1000, ge=1, le=MAX_LIMIT),
):
    """Bougies OHLCV triées par date croissante, paginées via `next_start`."""
    def aware(d: datetime | None) -> datetime | None:
        return d.replace(tzinfo=UTC) if d and d.tzinfo is None else d

    start, end = aware(start), aware(end)
    if start and end and start >= end:
        raise HTTPException(422, "`start` doit être antérieur à `end`")
    with _cursor(request) as cur:
        symbol = _require_symbol(cur, symbol)
        if interval == "1m":
            sql = ("SELECT open_time, open, high, low, close, volume, quote_volume, trades "
                   "FROM crypto.klines_1m WHERE symbol = %s")
            params: list = [symbol]
        else:
            sql = ("SELECT open_time, open, high, low, close, volume, quote_volume, trades "
                   "FROM crypto.klines_agg WHERE symbol = %s AND interval = %s")
            params = [symbol, interval]
        if start:
            sql += " AND open_time >= %s"
            params.append(start)
        if end:
            sql += " AND open_time < %s"
            params.append(end)
        sql += " ORDER BY open_time LIMIT %s"
        params.append(limit)
        cur.execute(sql, params)
        rows = cur.fetchall()
    next_start = rows[-1]["open_time"] + _STEP[interval] if len(rows) == limit else None
    return {"symbol": symbol, "interval": interval, "count": len(rows),
            "next_start": next_start, "data": rows}


@app.get("/prices/latest", response_model=list[schemas.LatestPrice], tags=["prix"])
def latest_prices(request: Request):
    """Dernier prix connu de chaque crypto."""
    with _cursor(request) as cur:
        cur.execute("""SELECT DISTINCT ON (symbol) symbol, open_time, close, volume
                       FROM crypto.klines_1m
                       WHERE open_time >= now() - interval '1 day'
                       ORDER BY symbol, open_time DESC""")
        return cur.fetchall()


@app.get("/prices/{symbol}/latest", response_model=schemas.LatestPrice, tags=["prix"])
def latest_price(request: Request, symbol: str):
    with _cursor(request) as cur:
        symbol = _require_symbol(cur, symbol)
        cur.execute("SELECT symbol, open_time, close, volume FROM crypto.klines_1m "
                    "WHERE symbol = %s ORDER BY open_time DESC LIMIT 1", (symbol,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"Aucune donnée pour {symbol}")
    return row


@app.get("/market/latest", response_model=list[schemas.MarketSnapshot], tags=["marché"])
def market_latest(request: Request):
    """Dernier instantané CoinGecko (prix, market cap, volume 24h) par crypto."""
    with _cursor(request) as cur:
        cur.execute("""SELECT DISTINCT ON (symbol) symbol, fetched_at, price_usd, market_cap_usd,
                              market_cap_rank, total_volume_usd, price_change_24h_pct
                       FROM crypto.market_snapshots
                       ORDER BY symbol, fetched_at DESC""")
        return cur.fetchall()


@app.get("/social/{symbol}", response_model=list[schemas.SocialMetric], tags=["marché"])
def social(request: Request, symbol: str, days: int = Query(30, ge=1, le=365)):
    with _cursor(request) as cur:
        symbol = _require_symbol(cur, symbol)
        cur.execute("""SELECT symbol, metric_date, twitter_followers, reddit_subscribers,
                              telegram_channel_users
                       FROM crypto.social_metrics
                       WHERE symbol = %s AND metric_date >= current_date - %s
                       ORDER BY metric_date""", (symbol, days))
        return cur.fetchall()


@app.get("/pipeline/status", response_model=list[schemas.PipelineStatus], tags=["monitoring"])
def pipeline_status(request: Request):
    """Monitoring : dernière exécution, fraîcheur et taux de succès 24 h de chaque pipeline."""
    with _cursor(request) as cur:
        cur.execute("""
            WITH recent AS (
              SELECT * FROM ops.pipeline_runs WHERE finished_at >= now() - interval '24 hours'
            ), last AS (
              SELECT DISTINCT ON (pipeline) pipeline, status, finished_at, rows_written, error
              FROM ops.pipeline_runs ORDER BY pipeline, finished_at DESC
            )
            SELECT l.pipeline,
                   l.status AS last_status,
                   l.finished_at AS last_finished_at,
                   EXTRACT(EPOCH FROM (now() - l.finished_at))::float AS seconds_since_last_run,
                   l.rows_written AS rows_written_last_run,
                   COALESCE(AVG((r.status = 'success')::int)::float, 0) AS success_rate_24h,
                   COUNT(r.id)::int AS runs_24h,
                   l.error AS last_error
            FROM last l LEFT JOIN recent r USING (pipeline)
            GROUP BY l.pipeline, l.status, l.finished_at, l.rows_written, l.error
            ORDER BY l.pipeline
        """)
        return cur.fetchall()
