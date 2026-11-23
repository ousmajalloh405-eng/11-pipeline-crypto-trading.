"""Logging structuré (clé=valeur) : lisible par un humain ET filtrable par une machine.

Dans Airflow, ces logs apparaissent directement dans l'onglet "Logs" de chaque tâche.
"""
from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    from crypto_pipeline.config import get_settings

    logger = logging.getLogger(name)
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(get_settings().log_level)
    return logger


def kv(**fields) -> str:
    """kv(rows=10, symbol='BTC') -> 'rows=10 symbol=BTC'"""
    return " ".join(f"{k}={v}" for k, v in fields.items())
