"""Tests d'intégration contre un vrai PostgreSQL (clients API simulés)."""
from datetime import UTC, datetime, timedelta

import pytest
from conftest import make_raw_kline, minute_ago

from crypto_pipeline import db, ingest, quality
from crypto_pipeline.config import Coin


class FakeBinance:
    """Retourne 4 bougies fermées + 1 bougie en cours ; BAD échoue systématiquement."""
    def __init__(self, fail=()):
        self.fail = set(fail)

    def klines(self, binance_symbol, limit=5, start_ms=None, end_ms=None, interval="1m"):
        if binance_symbol in self.fail:
            raise RuntimeError("boom")
        now = datetime.now(UTC).replace(second=0, microsecond=0)
        return [make_raw_kline(now - timedelta(minutes=n), price=100 + n) for n in (4, 3, 2, 1, 0)]


COINS = [Coin("BTC", "Bitcoin", "bitcoin"), Coin("ETH", "Ethereum", "ethereum")]


def count(sql):
    return db.fetch_all(sql)[0]["n"]


def test_seed_assets_loads_50_coins(pg):
    assert count("SELECT count(*) AS n FROM crypto.assets") == 50


def test_ingest_klines_stores_only_closed_candles_and_is_idempotent(pg):
    result = ingest.ingest_klines(COINS, client=FakeBinance())
    assert result["symbols_ok"] == 2 and result["symbols_failed"] == 0
    assert result["rows_written"] == 8                          # 2 cryptos x 4 bougies fermées
    again = ingest.ingest_klines(COINS, client=FakeBinance())
    assert again["rows_written"] == 0                           # relancer = aucun doublon
    assert count("SELECT count(*) AS n FROM crypto.klines_1m") == 8


def test_ingest_klines_partial_failure_does_not_block_others(pg):
    result = ingest.ingest_klines(COINS, client=FakeBinance(fail={"ETHUSDT"}))
    assert result["symbols_ok"] == 1 and result["symbols_failed"] == 1
    run = db.fetch_all("SELECT * FROM ops.pipeline_runs ORDER BY id DESC LIMIT 1")[0]
    assert run["status"] == "partial" and "ETH" in run["error"]


def test_ingest_klines_all_failing_raises_so_airflow_retries(pg):
    with pytest.raises(RuntimeError):
        ingest.ingest_klines(COINS, client=FakeBinance(fail={"BTCUSDT", "ETHUSDT"}))
    assert db.fetch_all("SELECT status FROM ops.pipeline_runs")[0]["status"] == "failed"


def test_ingest_market(pg):
    class FakeGecko:
        def markets(self, ids):
            return [{"id": "bitcoin", "current_price": 65000, "market_cap": 1e12, "market_cap_rank": 1,
                     "total_volume": 1e10, "price_change_percentage_24h": 2.5, "circulating_supply": 19e6}]
    result = ingest.ingest_market(COINS, client=FakeGecko())
    assert result == {"rows_written": 1, "coins_ok": 1, "coins_missing": 1}   # ETH absent de la réponse
    assert db.fetch_all("SELECT status FROM ops.pipeline_runs")[0]["status"] == "partial"


def test_ingest_social(pg):
    class FakeGecko:
        def coin_community(self, coin_id):
            return {"community_data": {"twitter_followers": 100, "reddit_subscribers": 50}}
    ingest.ingest_social(COINS, client=FakeGecko())
    ingest.ingest_social(COINS, client=FakeGecko())            # relance du même jour : upsert
    assert count("SELECT count(*) AS n FROM crypto.social_metrics") == 2


def test_aggregate_builds_5m_candles(pg):
    ingest.ingest_klines(COINS, client=FakeBinance())
    result = ingest.aggregate(hours_back=1)
    assert result["rows_written"] > 0
    rows = db.fetch_all("SELECT * FROM crypto.klines_agg WHERE symbol='BTC' AND interval='1h'")
    assert rows and sum(r["candles"] for r in rows) == 4


def test_export_parquet_roundtrip(pg, tmp_path):
    import pandas as pd
    day = datetime.now(UTC).date()
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    rows = [("BTC", start + timedelta(minutes=i), 100, 101, 99, 100, 1, 100, 5) for i in range(3)]
    db.insert_klines(rows)
    result = ingest.export_parquet(day)
    assert result["rows_written"] == 3
    df = pd.read_parquet(result["path"])
    assert len(df) == 3 and set(df["symbol"].astype(str)) == {"BTC"}
    ingest.export_parquet(day)                                  # relance : écrase, pas de doublon
    assert len(pd.read_parquet(result["path"])) == 3


def test_quality_check_passes_on_fresh_complete_data(pg):
    db.insert_klines([("BTC", minute_ago(n), 100, 101, 99, 100, 1, 100, 5) for n in range(1, 30)])
    assert quality.check_klines(hours=1)["worst_gap_ratio"] == 0


def test_quality_check_fails_on_stale_data(pg):
    rows = [("BTC", minute_ago(n), 100, 101, 99, 100, 1, 100, 5) for n in range(30, 50)]
    db.insert_klines(rows)
    with pytest.raises(RuntimeError, match="périmées"):
        quality.check_klines(hours=1)


def test_quality_check_fails_on_gaps(pg):
    rows = [("BTC", minute_ago(n), 100, 101, 99, 100, 1, 100, 5) for n in (1, 2, 3, 20, 21)]
    db.insert_klines(rows)
    with pytest.raises(RuntimeError, match="manquantes"):
        quality.check_klines(hours=1)
