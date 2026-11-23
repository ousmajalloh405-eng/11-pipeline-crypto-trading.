"""Processing : agrégation pandas des bougies 1m en 5m / 15m / 1h / 1d.

Fenêtre glissante recalculée chaque heure ; les écritures sont des upserts.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_aggregate_hourly",
    description="Agrégats OHLCV 5m/15m/1h/1d calculés avec pandas",
    schedule="5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(minutes=1)},
    tags=["crypto", "processing"],
)
def crypto_aggregate_hourly():
    @task
    def aggregate_candles() -> dict:
        from crypto_pipeline.ingest import aggregate
        return aggregate(hours_back=3)

    aggregate_candles()


crypto_aggregate_hourly()
