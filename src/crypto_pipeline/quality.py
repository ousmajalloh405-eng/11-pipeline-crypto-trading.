"""Contrôles qualité sur les données stockées."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_pipeline import db
from crypto_pipeline.logging_conf import get_logger, kv

log = get_logger("crypto_pipeline.quality")


def check_klines(hours: int = 1, max_gap_ratio: float = 0.05, max_stale_minutes: int = 5) -> dict:
    """Vérifie fraîcheur et complétude des bougies 1m.

    - Fraîcheur : la dernière bougie ne doit pas avoir plus de `max_stale_minutes`.
    - Complétude : dans la fenêtre, % de minutes manquantes par crypto <= `max_gap_ratio`.
    Lève une exception si un seuil est dépassé -> la tâche Airflow passe en rouge
    (c'est le principe d'un "circuit de contrôle" : mieux vaut alerter que de servir des trous).
    """
    now = datetime.now(UTC)
    since = now - timedelta(hours=hours)
    stats = db.fetch_all(
        """SELECT symbol, count(*) AS actual, min(open_time) AS first_t, max(open_time) AS last_t
           FROM crypto.klines_1m WHERE open_time >= %s GROUP BY symbol""",
        [since],
    )
    problems: list[str] = []
    if not stats:
        raise RuntimeError("Aucune bougie dans la fenêtre : pipeline à l'arrêt ?")

    newest = max(r["last_t"] for r in stats)
    stale = (now - newest).total_seconds() / 60
    if stale > max_stale_minutes:
        problems.append(f"données périmées : dernière bougie il y a {stale:.0f} min")

    worst = 0.0
    for r in stats:
        expected = int((r["last_t"] - r["first_t"]).total_seconds() // 60) + 1
        ratio = 1 - r["actual"] / expected if expected else 0.0
        worst = max(worst, ratio)
        if ratio > max_gap_ratio:
            problems.append(f"{r['symbol']}: {ratio:.0%} de minutes manquantes")

    result = {"symbols": len(stats), "stale_minutes": round(stale, 1), "worst_gap_ratio": round(worst, 3)}
    if problems:
        log.error(kv(event="quality_failed", problems=" | ".join(problems[:5]), **result))
        raise RuntimeError("Contrôle qualité échoué : " + " | ".join(problems[:5]))
    log.info(kv(event="quality_ok", **result))
    return result
