"""Accès PostgreSQL (psycopg2) : connexion, upserts idempotents, suivi des runs."""
from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from crypto_pipeline.config import Coin, get_settings


@contextmanager
def get_conn() -> Iterator[psycopg2.extensions.connection]:
    """Connexion avec commit automatique, rollback en cas d'erreur."""
    conn = psycopg2.connect(get_settings().database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_assets(coins: Sequence[Coin]) -> int:
    rows = [(c.symbol, c.name, c.binance_symbol, c.coingecko_id) for c in coins]
    sql = """
        INSERT INTO crypto.assets (symbol, name, binance_symbol, coingecko_id)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE
           SET name = EXCLUDED.name,
               binance_symbol = EXCLUDED.binance_symbol,
               coingecko_id = EXCLUDED.coingecko_id
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
    return len(rows)


def _bulk_insert(sql: str, rows: Sequence[tuple]) -> int:
    if not rows:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=2000)
        return cur.rowcount


def insert_klines(rows: Sequence[tuple]) -> int:
    """rows : (symbol, open_time, o, h, l, c, volume, quote_volume, trades).

    ON CONFLICT DO NOTHING : une bougie fermée ne change plus, donc ré-insérer
    la même bougie est sans effet -> le DAG peut être relancé sans risque.
    """
    sql = """
        INSERT INTO crypto.klines_1m
            (symbol, open_time, open, high, low, close, volume, quote_volume, trades)
        VALUES %s
        ON CONFLICT (symbol, open_time) DO NOTHING
    """
    return _bulk_insert(sql, rows)


def insert_market_snapshots(rows: Sequence[tuple]) -> int:
    sql = """
        INSERT INTO crypto.market_snapshots
            (symbol, fetched_at, price_usd, market_cap_usd, market_cap_rank,
             total_volume_usd, price_change_24h_pct, circulating_supply)
        VALUES %s
        ON CONFLICT (symbol, fetched_at) DO NOTHING
    """
    return _bulk_insert(sql, rows)


def upsert_social_metrics(rows: Sequence[tuple]) -> int:
    sql = """
        INSERT INTO crypto.social_metrics
            (symbol, metric_date, twitter_followers, reddit_subscribers, telegram_channel_users)
        VALUES %s
        ON CONFLICT (symbol, metric_date) DO UPDATE
           SET twitter_followers = EXCLUDED.twitter_followers,
               reddit_subscribers = EXCLUDED.reddit_subscribers,
               telegram_channel_users = EXCLUDED.telegram_channel_users
    """
    return _bulk_insert(sql, rows)


def upsert_klines_agg(rows: Sequence[tuple]) -> int:
    """Les agrégats peuvent changer tant que la période n'est pas terminée -> DO UPDATE."""
    sql = """
        INSERT INTO crypto.klines_agg
            (symbol, interval, open_time, open, high, low, close,
             volume, quote_volume, trades, candles)
        VALUES %s
        ON CONFLICT (symbol, interval, open_time) DO UPDATE
           SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
               close = EXCLUDED.close, volume = EXCLUDED.volume,
               quote_volume = EXCLUDED.quote_volume, trades = EXCLUDED.trades,
               candles = EXCLUDED.candles
    """
    return _bulk_insert(sql, rows)


def fetch_all(sql: str, params: Iterable | None = None) -> list[dict]:
    from psycopg2.extras import RealDictCursor

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, tuple(params) if params else None)
        return [dict(r) for r in cur.fetchall()]


def record_run(
    pipeline: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    rows_written: int = 0,
    symbols_ok: int = 0,
    symbols_failed: int = 0,
    error: str | None = None,
) -> None:
    """Journalise une exécution dans ops.pipeline_runs (base du monitoring)."""
    sql = """
        INSERT INTO ops.pipeline_runs
            (pipeline, status, started_at, finished_at, rows_written,
             symbols_ok, symbols_failed, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (pipeline, status, started_at, finished_at, rows_written,
                          symbols_ok, symbols_failed, error))
