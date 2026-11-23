"""Modèles de réponse de l'API (validés et documentés automatiquement par FastAPI)."""
from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class Asset(BaseModel):
    symbol: str
    name: str
    binance_symbol: str
    coingecko_id: str
    active: bool


class Candle(BaseModel):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int


class KlinesResponse(BaseModel):
    symbol: str
    interval: str
    count: int
    next_start: datetime | None = Field(
        None, description="À passer en `start` pour obtenir la page suivante (null = fin des données)")
    data: list[Candle]


class LatestPrice(BaseModel):
    symbol: str
    open_time: datetime
    close: float
    volume: float


class MarketSnapshot(BaseModel):
    symbol: str
    fetched_at: datetime
    price_usd: float | None
    market_cap_usd: float | None
    market_cap_rank: int | None
    total_volume_usd: float | None
    price_change_24h_pct: float | None


class SocialMetric(BaseModel):
    symbol: str
    metric_date: date
    twitter_followers: int | None
    reddit_subscribers: int | None
    telegram_channel_users: int | None


class PipelineStatus(BaseModel):
    pipeline: str
    last_status: str
    last_finished_at: datetime
    seconds_since_last_run: float
    rows_written_last_run: int
    success_rate_24h: float
    runs_24h: int
    last_error: str | None
