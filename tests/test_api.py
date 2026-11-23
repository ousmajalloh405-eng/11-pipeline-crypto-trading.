"""Tests de l'API FastAPI contre un vrai PostgreSQL."""
import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import minute_ago
from fastapi.testclient import TestClient

from crypto_pipeline import db, ingest


@pytest.fixture()
def client(pg):
    rows = [("BTC", minute_ago(n), 100 + n, 102 + n, 98 + n, 101 + n, 1.5, 150, 7) for n in range(60, 0, -1)]
    db.insert_klines(rows)
    ingest.aggregate(hours_back=2)
    db.insert_market_snapshots([("BTC", minute_ago(0), 65000, 1.2e12, 1, 3e10, 1.5, 19e6)])
    db.record_run("binance_klines", "success", minute_ago(1), minute_ago(1), 250, 50, 0)
    db.record_run("binance_klines", "failed", minute_ago(2), minute_ago(2), error="boom")
    from crypto_pipeline.api.main import app
    with TestClient(app) as c:
        yield c


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    assert "X-Process-Time-ms" in r.headers


def test_assets(client):
    data = client.get("/assets").json()
    assert len(data) == 50 and data[0]["symbol"] == "AAVE"


def test_klines_returns_ordered_candles(client):
    r = client.get("/klines/btc", params={"limit": 10})              # minuscules acceptées
    body = r.json()
    assert r.status_code == 200 and body["count"] == 10 and body["symbol"] == "BTC"
    times = [c["open_time"] for c in body["data"]]
    assert times == sorted(times)


def test_klines_pagination_walks_all_data_without_overlap(client):
    seen, start = [], None
    for _ in range(10):
        params = {"limit": 25, **({"start": start} if start else {})}
        body = client.get("/klines/BTC", params=params).json()
        seen += [c["open_time"] for c in body["data"]]
        start = body["next_start"]
        if start is None:
            break
    assert len(seen) == 60 and len(set(seen)) == 60                   # 60 bougies, aucune en double


def test_klines_aggregated_interval(client):
    body = client.get("/klines/BTC", params={"interval": "1h"}).json()
    assert body["interval"] == "1h" and body["count"] >= 1


def test_klines_time_filters_and_validation(client):
    now = datetime.now(UTC)
    ok = client.get("/klines/BTC", params={"start": (now - timedelta(minutes=10)).isoformat(),
                                           "end": now.isoformat()}).json()
    assert 0 < ok["count"] <= 10
    assert client.get("/klines/BTC", params={"start": now.isoformat(),
                                             "end": (now - timedelta(hours=1)).isoformat()}).status_code == 422
    assert client.get("/klines/BTC", params={"interval": "7m"}).status_code == 422
    assert client.get("/klines/BTC", params={"limit": 999999}).status_code == 422


def test_unknown_symbol_is_404(client):
    assert client.get("/klines/NOPE").status_code == 404
    assert client.get("/prices/NOPE/latest").status_code == 404


def test_latest_prices(client):
    one = client.get("/prices/BTC/latest").json()
    assert one["symbol"] == "BTC" and one["close"] == 102.0           # bougie n=1 -> 101 + 1
    allp = client.get("/prices/latest").json()
    assert [p["symbol"] for p in allp] == ["BTC"]


def test_market_latest(client):
    data = client.get("/market/latest").json()
    assert data[0]["symbol"] == "BTC" and data[0]["market_cap_rank"] == 1


def test_pipeline_status_reports_freshness_and_success_rate(client):
    status = client.get("/pipeline/status").json()
    row = next(s for s in status if s["pipeline"] == "binance_klines")
    assert row["last_status"] == "success" and row["runs_24h"] == 2
    assert row["success_rate_24h"] == 0.5 and row["seconds_since_last_run"] < 300


def test_latency_is_good_enough_for_backtesting(client):
    """Critère de la fiche : API performante. Seuil large (CI lente) : p95 < 150 ms."""
    client.get("/klines/BTC")                                          # échauffement
    samples = []
    for _ in range(40):
        t0 = time.perf_counter()
        assert client.get("/klines/BTC", params={"limit": 1000}).status_code == 200
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    assert samples[int(len(samples) * 0.95) - 1] < 150
