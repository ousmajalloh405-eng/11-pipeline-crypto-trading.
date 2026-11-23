"""Jobs de collecte et de traitement : la logique métier appelée par les DAGs Airflow.

Les DAGs restent minces (planification + dépendances) ; tout ce qui est testable
vit ici. Chaque job journalise son résultat dans ops.pipeline_runs.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from crypto_pipeline import db, transform
from crypto_pipeline.clients.binance import BinanceClient
from crypto_pipeline.clients.coingecko import CoinGeckoClient
from crypto_pipeline.config import Coin, get_settings, load_coins
from crypto_pipeline.logging_conf import get_logger, kv

log = get_logger("crypto_pipeline.ingest")


def _now() -> datetime:
    return datetime.now(UTC)


def _status(ok: int, failed: int) -> str:
    if failed == 0:
        return "success"
    return "partial" if ok > 0 else "failed"


def seed_assets() -> int:
    """Charge le référentiel config/coins.yaml dans crypto.assets."""
    n = db.upsert_assets(load_coins())
    log.info(kv(event="assets_seeded", count=n))
    return n


# --------------------------------------------------------------------------
# 1) Bougies 1 minute (Binance)
# --------------------------------------------------------------------------
def ingest_klines(coins: list[Coin] | None = None, limit: int = 5,
                  client: BinanceClient | None = None) -> dict:
    """Récupère les `limit` dernières bougies de chaque crypto, en parallèle.

    limit=5 (et non 1) : si une exécution est ratée, la suivante comble le trou
    automatiquement ("auto-guérison"). Les doublons sont sans effet (idempotence).
    Une crypto en erreur ne bloque pas les autres : le run finit en "partial".
    """
    started = _now()
    coins = coins or load_coins()
    client = client or BinanceClient()
    now_ms = int(started.timestamp() * 1000)
    rows: list[tuple] = []
    failed: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=get_settings().max_workers) as pool:
        futures = {pool.submit(client.klines, c.binance_symbol, limit=limit): c for c in coins}
        for fut in as_completed(futures):
            coin = futures[fut]
            try:
                rows.extend(transform.parse_klines(coin.symbol, fut.result(), now_ms))
            except Exception as exc:  # noqa: BLE001 - on isole l'échec d'une seule crypto
                failed[coin.symbol] = str(exc)
                log.warning(kv(event="klines_failed", symbol=coin.symbol, error=str(exc)[:150]))

    written = db.insert_klines(rows)
    ok = len(coins) - len(failed)
    result = {"rows_fetched": len(rows), "rows_written": written,
              "symbols_ok": ok, "symbols_failed": len(failed)}
    db.record_run("binance_klines", _status(ok, len(failed)), started, _now(),
                  written, ok, len(failed), "; ".join(f"{s}: {e}" for s, e in failed.items())[:1000] or None)
    log.info(kv(event="klines_ingested", **result))
    if ok == 0:
        raise RuntimeError(f"Aucune crypto collectée ({len(failed)} erreurs)")
    return result


# --------------------------------------------------------------------------
# 2) Historique (backfill) : remplir le passé
# --------------------------------------------------------------------------
def backfill_klines(days: int = 1, coins: list[Coin] | None = None,
                    client: BinanceClient | None = None) -> dict:
    """Charge `days` jours d'historique par pages de 1000 bougies (limite Binance)."""
    started = _now()
    coins = coins or load_coins()
    client = client or BinanceClient()
    end = started.replace(second=0, microsecond=0)
    start_ms = int((end - timedelta(days=days)).timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    total, failed = 0, {}

    for coin in coins:
        try:
            cursor = start_ms
            while cursor < end_ms:
                raw = client.klines(coin.binance_symbol, limit=1000, start_ms=cursor, end_ms=end_ms)
                if not raw:
                    break
                total += db.insert_klines(transform.parse_klines(coin.symbol, raw, end_ms))
                cursor = int(raw[-1][0]) + 60_000       # page suivante : après la dernière bougie
                time.sleep(0.05)                       # politesse envers l'API
        except Exception as exc:  # noqa: BLE001
            failed[coin.symbol] = str(exc)
            log.warning(kv(event="backfill_failed", symbol=coin.symbol, error=str(exc)[:150]))

    ok = len(coins) - len(failed)
    db.record_run("binance_backfill", _status(ok, len(failed)), started, _now(),
                  total, ok, len(failed), "; ".join(failed)[:1000] or None)
    log.info(kv(event="backfill_done", rows_written=total, symbols_ok=ok, symbols_failed=len(failed)))
    return {"rows_written": total, "symbols_ok": ok, "symbols_failed": len(failed)}


# --------------------------------------------------------------------------
# 3) Marché (CoinGecko) : prix, market cap, volume
# --------------------------------------------------------------------------
def ingest_market(coins: list[Coin] | None = None, client: CoinGeckoClient | None = None) -> dict:
    started = _now()
    coins = coins or load_coins()
    client = client or CoinGeckoClient()
    id_to_symbol = {c.coingecko_id: c.symbol for c in coins}
    try:
        payload = client.markets(list(id_to_symbol))
        # Horodatage arrondi à la minute : deux appels dans la même minute = même clé (idempotent)
        fetched_at = started.replace(second=0, microsecond=0)
        written = db.insert_market_snapshots(transform.parse_markets(payload, id_to_symbol, fetched_at))
    except Exception as exc:
        db.record_run("coingecko_market", "failed", started, _now(), error=str(exc)[:1000])
        raise
    ok = len({p.get("id") for p in payload} & set(id_to_symbol))
    missing = len(coins) - ok
    db.record_run("coingecko_market", _status(ok, missing), started, _now(), written, ok, missing)
    log.info(kv(event="market_ingested", rows_written=written, coins_ok=ok, coins_missing=missing))
    return {"rows_written": written, "coins_ok": ok, "coins_missing": missing}


def ingest_social(coins: list[Coin] | None = None, client: CoinGeckoClient | None = None) -> dict:
    """Métriques sociales du jour : 1 appel par crypto, espacés (limite de débit)."""
    started = _now()
    coins = coins or load_coins()
    client = client or CoinGeckoClient()
    today = started.date()
    rows, failed = [], {}
    for coin in coins:
        try:
            rows.append(transform.parse_social(coin.symbol, client.coin_community(coin.coingecko_id), today))
        except Exception as exc:  # noqa: BLE001
            failed[coin.symbol] = str(exc)
            log.warning(kv(event="social_failed", symbol=coin.symbol, error=str(exc)[:150]))
    written = db.upsert_social_metrics(rows)
    ok = len(coins) - len(failed)
    db.record_run("coingecko_social", _status(ok, len(failed)), started, _now(),
                  written, ok, len(failed), "; ".join(failed)[:1000] or None)
    log.info(kv(event="social_ingested", rows_written=written, symbols_ok=ok, symbols_failed=len(failed)))
    if ok == 0:
        raise RuntimeError("Aucune métrique sociale collectée")
    return {"rows_written": written, "symbols_ok": ok, "symbols_failed": len(failed)}


# --------------------------------------------------------------------------
# 4) Processing : agrégation pandas (5m / 15m / 1h / 1d)
# --------------------------------------------------------------------------
def aggregate(hours_back: int = 3, intervals: tuple[str, ...] = ("5m", "15m", "1h", "1d")) -> dict:
    """Recalcule les agrégats sur une fenêtre glissante.

    On recalcule des périodes COMPLÈTES : la fenêtre est alignée sur la journée
    pour ne pas produire de bougie "1d" tronquée à cause du début de fenêtre.
    """
    started = _now()
    window_start = (started - timedelta(hours=hours_back)).replace(minute=0, second=0, microsecond=0)
    day_start = window_start.replace(hour=0)
    rows = db.fetch_all(
        """SELECT symbol, open_time, open, high, low, close, volume, quote_volume, trades
           FROM crypto.klines_1m WHERE open_time >= %s ORDER BY symbol, open_time""",
        [day_start],
    )
    df = pd.DataFrame(rows)
    if df.empty:
        db.record_run("klines_aggregate", "success", started, _now())
        return {"rows_written": 0}
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    written = sum(db.upsert_klines_agg(transform.aggregate_klines(df, itv)) for itv in intervals)
    db.record_run("klines_aggregate", "success", started, _now(), written)
    log.info(kv(event="aggregated", rows_written=written, source_rows=len(df)))
    return {"rows_written": written, "source_rows": len(df)}


# --------------------------------------------------------------------------
# 5) Parquet : export quotidien (historique analytique)
# --------------------------------------------------------------------------
def export_parquet(day: date | None = None) -> dict:
    """Exporte les bougies 1m d'un jour en Parquet partitionné date=/symbol=.

    Pourquoi Parquet en plus de PostgreSQL ? Format colonnaire compressé :
    ~10x plus petit qu'un CSV et lisible directement par pandas/Spark/DuckDB
    pour le backtesting, sans solliciter la base de production.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    started = _now()
    day = day or (started - timedelta(days=1)).date()
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    rows = db.fetch_all(
        """SELECT symbol, open_time, open, high, low, close, volume, quote_volume, trades
           FROM crypto.klines_1m WHERE open_time >= %s AND open_time < %s
           ORDER BY symbol, open_time""",
        [start, start + timedelta(days=1)],
    )
    if not rows:
        log.warning(kv(event="parquet_empty", day=day))
        db.record_run("parquet_export", "success", started, _now())
        return {"rows_written": 0, "day": str(day)}
    df = pd.DataFrame(rows)
    for col in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["date"] = day.isoformat()
    root = get_settings().parquet_dir / "klines_1m"
    pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False), root_path=str(root),
                        partition_cols=["date", "symbol"], existing_data_behavior="delete_matching")
    db.record_run("parquet_export", "success", started, _now(), len(df))
    log.info(kv(event="parquet_exported", day=day, rows=len(df), path=root))
    return {"rows_written": len(df), "day": str(day), "path": str(root)}
