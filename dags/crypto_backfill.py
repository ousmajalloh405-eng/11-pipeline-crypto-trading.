"""Backfill manuel : charge N jours d'historique Binance (déclenché à la main).

Airflow UI -> DAG `crypto_backfill` -> "Trigger DAG w/ config" -> days = 7
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_backfill",
    description="Chargement manuel de l'historique 1m (paramètre : days)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"days": Param(1, type="integer", minimum=1, maximum=30,
                          description="Nombre de jours d'historique à charger")},
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(minutes=1)},
    tags=["crypto", "binance", "backfill"],
)
def crypto_backfill():
    @task(execution_timeout=timedelta(hours=1))
    def backfill(params=None) -> dict:
        from crypto_pipeline.ingest import backfill_klines
        return backfill_klines(days=int(params["days"]))

    backfill()


crypto_backfill()
