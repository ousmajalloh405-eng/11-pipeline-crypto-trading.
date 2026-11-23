"""Configuration centralisée, lue depuis les variables d'environnement.

Un seul endroit pour tous les paramètres : pas de valeur codée en dur
dans le reste du code (principe "12-factor app").
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Coin:
    symbol: str           # BTC
    name: str             # Bitcoin
    coingecko_id: str     # bitcoin

    @property
    def binance_symbol(self) -> str:   # BTCUSDT
        return f"{self.symbol}USDT"


@dataclass(frozen=True)
class Settings:
    database_url: str
    binance_base_url: str
    coingecko_base_url: str
    coingecko_api_key: str | None
    coingecko_min_interval_s: float
    parquet_dir: Path
    coins_file: Path
    http_timeout_s: float
    max_workers: int
    log_level: str


def get_settings() -> Settings:
    root = Path(__file__).resolve().parents[2]
    return Settings(
        database_url=os.getenv(
            "CRYPTO_DATABASE_URL",
            "postgresql://crypto:crypto@localhost:5432/crypto",
        ),
        # data-api.binance.vision = endpoint public "market data only" si
        # api.binance.com est bloqué dans votre pays (erreur HTTP 451).
        binance_base_url=os.getenv("BINANCE_BASE_URL", "https://api.binance.com"),
        coingecko_base_url=os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
        coingecko_api_key=os.getenv("COINGECKO_API_KEY") or None,
        # L'offre gratuite CoinGecko tolère ~30 appels/min : 2.5 s d'écart = marge de sécurité.
        coingecko_min_interval_s=float(os.getenv("COINGECKO_MIN_INTERVAL_S", "2.5")),
        parquet_dir=Path(os.getenv("PARQUET_DIR", str(root / "data" / "parquet"))),
        coins_file=Path(os.getenv("COINS_FILE", str(root / "config" / "coins.yaml"))),
        http_timeout_s=float(os.getenv("HTTP_TIMEOUT_S", "10")),
        max_workers=int(os.getenv("MAX_WORKERS", "8")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )


def load_coins(path: Path | None = None) -> list[Coin]:
    path = path or get_settings().coins_file
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    coins = [Coin(c["symbol"], c["name"], c["coingecko_id"]) for c in raw["coins"]]
    if len({c.symbol for c in coins}) != len(coins):
        raise ValueError("coins.yaml contient des symboles en double")
    return coins
