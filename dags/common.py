"""Paramètres communs à tous les DAGs."""
from __future__ import annotations

import logging
from datetime import timedelta

log = logging.getLogger("airflow.task")


def alert_on_failure(context: dict) -> None:
    """Appelé quand une tâche échoue DÉFINITIVEMENT (après tous les retries).

    Ici on journalise ; en production on brancherait Slack / email / PagerDuty.
    """
    ti = context["task_instance"]
    log.error("PIPELINE_FAILURE dag=%s task=%s run=%s try=%s error=%s",
              ti.dag_id, ti.task_id, context.get("run_id"), ti.try_number,
              str(context.get("exception"))[:300])


DEFAULT_ARGS = {
    "owner": "ousmane",
    "retries": 3,                                  # 3 nouvelles tentatives avant d'échouer
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,             # 10 s, 20 s, 40 s...
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}
