"""Collecte des bougies 1 minute de 50 cryptos (Binance) — toutes les minutes.

collect (Binance -> PostgreSQL)  ->  quality_check (fraîcheur + complétude)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_binance_klines_1m",
    description="Bougies 1m de 50 cryptos depuis Binance, chaque minute",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,                 # pas de rattrapage massif au démarrage (le backfill s'en charge)
    max_active_runs=1,             # jamais deux collectes en même temps
    dagrun_timeout=timedelta(seconds=55),   # une collecte doit finir avant la minute suivante
    default_args={**DEFAULT_ARGS, "retries": 2, "retry_delay": timedelta(seconds=5)},
    tags=["crypto", "binance", "ingestion"],
)
def crypto_binance_klines_1m():
    @task(execution_timeout=timedelta(seconds=50))
    def collect() -> dict:
        from crypto_pipeline.ingest import ingest_klines
        return ingest_klines(limit=5)

    @task(retries=0)
    def quality_check(_collected: dict) -> dict:
        from crypto_pipeline.quality import check_klines
        return check_klines(hours=1)

    quality_check(collect())


crypto_binance_klines_1m()
