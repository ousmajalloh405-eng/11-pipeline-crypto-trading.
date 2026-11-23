"""Instantanés de marché (prix, market cap, volume 24h) via CoinGecko — toutes les 5 minutes.

Un seul appel API pour les 50 cryptos (endpoint /coins/markets).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_coingecko_market_5m",
    description="Prix / market cap / volume 24h depuis CoinGecko, toutes les 5 min",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(seconds=30)},
    tags=["crypto", "coingecko", "ingestion"],
)
def crypto_coingecko_market_5m():
    @task
    def collect_market() -> dict:
        from crypto_pipeline.ingest import ingest_market
        return ingest_market()

    collect_market()


crypto_coingecko_market_5m()
