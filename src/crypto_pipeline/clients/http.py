"""Session HTTP robuste : timeouts, retries avec backoff exponentiel, gestion du 429.

Pourquoi : une API publique tombe, ralentit ou limite le débit. Sans retry, un
simple hoquet réseau ferait échouer toute la collecte de la minute.
"""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ApiError(RuntimeError):
    """Erreur API non récupérable (4xx hors 429, réponse invalide...)."""


def build_session(total_retries: int = 3, backoff: float = 0.5) -> requests.Session:
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,                      # attentes : 0.5 s, 1 s, 2 s...
        status_forcelist=(429, 500, 502, 503, 504),  # erreurs temporaires -> on réessaie
        allowed_methods=("GET",),
        respect_retry_after_header=True,             # si l'API dit "reviens dans 30 s", on obéit
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "crypto-pipeline/1.0 (portfolio project)"})
    return session


def get_json(session: requests.Session, url: str, *, params: dict | None = None,
             headers: dict | None = None, timeout: float = 10.0):
    try:
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise ApiError(f"Échec réseau sur {url}: {exc}") from exc
    if resp.status_code == 451:
        raise ApiError(
            "HTTP 451 : l'API est bloquée dans votre région. "
            "Définissez BINANCE_BASE_URL=https://data-api.binance.vision"
        )
    if resp.status_code != 200:
        raise ApiError(f"HTTP {resp.status_code} sur {url}: {resp.text[:200]}")
    try:
        return resp.json()
    except ValueError as exc:
        raise ApiError(f"Réponse non-JSON sur {url}") from exc
