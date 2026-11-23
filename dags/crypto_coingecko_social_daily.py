"""Métriques sociales quotidiennes (Twitter/Reddit/Telegram) via CoinGecko.

50 appels espacés de 2,5 s (limite du plan gratuit) => ~2 minutes.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_coingecko_social_daily",
    description="Métriques sociales quotidiennes depuis CoinGecko",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(minutes=2)},
    tags=["crypto", "coingecko", "social"],
)
def crypto_coingecko_social_daily():
    @task(execution_timeout=timedelta(minutes=15))
    def collect_social() -> dict:
        from crypto_pipeline.ingest import ingest_social
        return ingest_social()

    collect_social()


crypto_coingecko_social_daily()
