from datetime import UTC, date, datetime
from decimal import Decimal

import pandas as pd
from conftest import make_raw_kline

from crypto_pipeline import transform

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def test_parse_klines_converts_closed_candle():
    raw = [make_raw_kline(T0, price=100)]
    now_ms = int(T0.timestamp() * 1000) + 120_000
    rows = transform.parse_klines("BTC", raw, now_ms)
    assert len(rows) == 1
    symbol, open_time, o, h, lo, c, vol, qvol, trades = rows[0]
    assert symbol == "BTC" and open_time == T0
    assert (o, h, lo, c) == (Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100.5"))
    assert trades == 10


def test_parse_klines_ignores_candle_still_open():
    raw = [make_raw_kline(T0)]
    now_ms = int(T0.timestamp() * 1000) + 30_000       # 30 s après l'ouverture : bougie en cours
    assert transform.parse_klines("BTC", raw, now_ms) == []


def test_parse_klines_rejects_invalid_values():
    bad_price = make_raw_kline(T0, price=0.5)           # low = -0.5 -> invalide
    bad_shape = [1, 2, 3]                               # trop court
    garbage = make_raw_kline(T0)
    garbage[1] = "pas-un-nombre"
    now_ms = int(T0.timestamp() * 1000) + 120_000
    assert transform.parse_klines("BTC", [bad_price, bad_shape, garbage], now_ms) == []


def test_parse_markets_filters_unknown_coins():
    payload = [
        {"id": "bitcoin", "current_price": 65000.5, "market_cap": 1.2e12, "market_cap_rank": 1,
         "total_volume": 3e10, "price_change_percentage_24h": -1.5, "circulating_supply": 19.7e6},
        {"id": "unknown-coin", "current_price": 1},
        {"id": "ethereum", "current_price": None},
    ]
    rows = transform.parse_markets(payload, {"bitcoin": "BTC", "ethereum": "ETH"}, T0)
    assert [r[0] for r in rows] == ["BTC", "ETH"]
    assert rows[0][2] == Decimal("65000.5")
    assert rows[1][2] is None                            # valeur absente -> None, pas d'erreur


def test_parse_social_handles_missing_fields():
    payload = {"community_data": {"twitter_followers": 1234, "reddit_subscribers": None}}
    assert transform.parse_social("BTC", payload, date(2026, 1, 1)) == ("BTC", date(2026, 1, 1), 1234, None, None)
    assert transform.parse_social("BTC", {}, date(2026, 1, 1))[2:] == (None, None, None)


def test_aggregate_klines_ohlcv_rules():
    times = pd.date_range(T0, periods=10, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "symbol": "BTC", "open_time": times,
        "open": [float(i) for i in range(10)], "high": [float(i) + 5 for i in range(10)],
        "low": [float(i) - 5 for i in range(10)], "close": [float(i) + 0.5 for i in range(10)],
        "volume": 1.0, "quote_volume": 10.0, "trades": 3,
    })
    rows = transform.aggregate_klines(df, "5m")
    assert len(rows) == 2
    first = rows[0]       # minutes 0..4
    _, itv, ot, o, h, lo, c, vol, qvol, trades, candles = first
    assert itv == "5m" and ot == T0
    assert o == 0.0                # open = première bougie
    assert h == 9.0                # high = max des highs (4 + 5)
    assert lo == -5.0               # low = min des lows
    assert c == 4.5                # close = dernière bougie
    assert vol == 5.0 and qvol == 50.0 and trades == 15 and candles == 5


def test_aggregate_klines_skips_empty_periods_and_unknown_interval():
    import pytest
    df = pd.DataFrame({
        "symbol": "BTC", "open_time": [T0, T0 + pd.Timedelta(minutes=30)],
        "open": [1.0, 2.0], "high": [1.0, 2.0], "low": [1.0, 2.0], "close": [1.0, 2.0],
        "volume": [1.0, 1.0], "quote_volume": [1.0, 1.0], "trades": [1, 1],
    })
    assert len(transform.aggregate_klines(df, "5m")) == 2    # pas de lignes pour les trous
    assert transform.aggregate_klines(df.iloc[0:0], "5m") == []
    with pytest.raises(ValueError):
        transform.aggregate_klines(df, "7m")
