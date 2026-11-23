"""Fixtures communes. Les tests qui touchent la base utilisent un vrai PostgreSQL
(pas de mock) : ils sont ignorés automatiquement si aucune base de test n'est joignable.

    CRYPTO_TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto_test pytest
"""
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

TEST_DSN = os.getenv("CRYPTO_TEST_DATABASE_URL", "postgresql://crypto:crypto@localhost:5432/crypto_test")


@pytest.fixture()
def pg(tmp_path, monkeypatch):
    try:
        conn = psycopg2.connect(TEST_DSN, connect_timeout=2)
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL de test indisponible")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS crypto CASCADE; DROP SCHEMA IF EXISTS ops CASCADE;")
        cur.execute((ROOT / "sql" / "01_schema.sql").read_text())
    conn.close()
    monkeypatch.setenv("CRYPTO_DATABASE_URL", TEST_DSN)
    monkeypatch.setenv("PARQUET_DIR", str(tmp_path / "parquet"))
    from crypto_pipeline import ingest
    ingest.seed_assets()
    return TEST_DSN


def make_raw_kline(open_dt: datetime, price: float = 100.0, trades: int = 10) -> list:
    """Fabrique une bougie au format brut Binance (tableau de 12 éléments)."""
    ot = int(open_dt.timestamp() * 1000)
    return [ot, str(price), str(price + 1), str(price - 1), str(price + 0.5), "2.5",
            ot + 59_999, str(price * 2.5), trades, "1.0", "100.0", "0"]


def minute_ago(n: int) -> datetime:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    return now - timedelta(minutes=n)
