"""Fonctions de transformation PURES (pas d'I/O) -> faciles à tester unitairement.

Principe : on sépare "aller chercher / écrire" (clients, db) de "transformer".
Ici, entrée = données brutes de l'API, sortie = lignes prêtes pour PostgreSQL.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

# Alias d'intervalle -> fréquence pandas
AGG_FREQ = {"5m": "5min", "15m": "15min", "1h": "1h", "1d": "1D"}


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _dec(value) -> Decimal | None:
    """Convertit proprement en Decimal ; None si absent/invalide (jamais d'exception)."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def parse_klines(symbol: str, raw: Iterable[list], now_ms: int) -> list[tuple]:
    """Réponse brute Binance -> lignes (symbol, open_time, o, h, l, c, vol, quote_vol, trades).

    Format d'une bougie Binance :
      [0] open_time(ms) [1] open [2] high [3] low [4] close [5] volume
      [6] close_time(ms) [7] quote_volume [8] nb_trades [9..11] (inutilisés ici)

    On ignore la bougie encore EN COURS (close_time >= maintenant) : ses valeurs
    changent encore, l'enregistrer fausserait l'historique.
    Les bougies invalides (prix <= 0, high < low) sont écartées.
    """
    rows: list[tuple] = []
    for k in raw:
        if len(k) < 9 or int(k[6]) >= now_ms:
            continue
        o, h, lo, c = (_dec(k[i]) for i in (1, 2, 3, 4))
        vol, qvol = _dec(k[5]), _dec(k[7])
        if None in (o, h, lo, c, vol, qvol) or lo <= 0 or h < lo:
            continue
        rows.append((symbol, _utc(int(k[0])), o, h, lo, c, vol, qvol, int(k[8])))
    return rows


def parse_markets(payload: list[dict], id_to_symbol: dict[str, str],
                  fetched_at: datetime) -> list[tuple]:
    """Réponse CoinGecko /coins/markets -> lignes market_snapshots.

    Les cryptos que l'on ne suit pas sont ignorées.
    """
    rows = []
    for item in payload:
        symbol = id_to_symbol.get(item.get("id"))
        if symbol is None:
            continue
        rows.append((
            symbol, fetched_at,
            _dec(item.get("current_price")), _dec(item.get("market_cap")),
            item.get("market_cap_rank"), _dec(item.get("total_volume")),
            _dec(item.get("price_change_percentage_24h")), _dec(item.get("circulating_supply")),
        ))
    return rows


def parse_social(symbol: str, payload: dict, day: date) -> tuple:
    """Réponse CoinGecko /coins/{id} -> ligne social_metrics (valeurs absentes = None)."""
    community = payload.get("community_data") or {}

    def as_int(v):
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return (
        symbol, day,
        as_int(community.get("twitter_followers")),
        as_int(community.get("reddit_subscribers")),
        as_int(community.get("telegram_channel_user_count")),
    )


def aggregate_klines(df: pd.DataFrame, interval: str) -> list[tuple]:
    """Agrège des bougies 1m en bougies 5m/15m/1h/1d.

    Règles OHLCV : open = première, high = max, low = min, close = dernière,
    volume = somme. C'est la même logique qu'un graphique de trading.
    """
    if interval not in AGG_FREQ:
        raise ValueError(f"Intervalle inconnu: {interval}")
    if df.empty:
        return []
    df = df.sort_values("open_time")
    grouped = df.groupby(["symbol", pd.Grouper(key="open_time", freq=AGG_FREQ[interval])])
    agg = grouped.agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"),
        quote_volume=("quote_volume", "sum"), trades=("trades", "sum"),
        candles=("open", "count"),
    ).reset_index()
    agg = agg[agg["candles"] > 0]          # supprime les périodes vides créées par le resample
    return [
        (r.symbol, interval, r.open_time.to_pydatetime(), r.open, r.high, r.low, r.close,
         r.volume, r.quote_volume, int(r.trades), int(r.candles))
        for r in agg.itertuples(index=False)
    ]
