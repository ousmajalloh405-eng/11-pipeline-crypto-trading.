"""Export quotidien des bougies 1m de la veille en Parquet (historique analytique)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common import DEFAULT_ARGS


@dag(
    dag_id="crypto_parquet_export_daily",
    description="Export Parquet partitionné date/symbol des bougies de la veille",
    schedule="15 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(minutes=5)},
    tags=["crypto", "parquet", "storage"],
)
def crypto_parquet_export_daily():
    @task
    def export(ds=None) -> dict:
        # Airflow : un run planifié à 00:15 le jour J porte ds = J-1 (début de l'intervalle
        # de données qu'il traite). On exporte donc exactement `ds` = la journée écoulée.
        from datetime import date

        from crypto_pipeline.ingest import export_parquet
        return export_parquet(date.fromisoformat(ds) if ds else None)

    export()


crypto_parquet_export_daily()
