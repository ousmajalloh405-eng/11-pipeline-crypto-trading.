"""Client CoinGecko (API publique gratuite, clé "demo" optionnelle)."""
from __future__ import annotations

import time
from collections.abc import Sequence

import requests

from crypto_pipeline.clients.http import ApiError, build_session, get_json
from crypto_pipeline.config import get_settings


class CoinGeckoClient:
    def __init__(self, session: requests.Session | None = None):
        self.settings = get_settings()
        self.session = session or build_session(total_retries=4, backoff=2.0)
        self._last_call = 0.0

    def _headers(self) -> dict:
        key = self.settings.coingecko_api_key
        return {"x-cg-demo-api-key": key} if key else {}

    def _throttle(self) -> None:
        """Espace les appels pour rester sous la limite du plan gratuit."""
        wait = self.settings.coingecko_min_interval_s - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def markets(self, coin_ids: Sequence[str]) -> list[dict]:
        """Prix, market cap, volume... pour toutes les cryptos en UN seul appel."""
        self._throttle()
        data = get_json(
            self.session,
            f"{self.settings.coingecko_base_url}/coins/markets",
            params={"vs_currency": "usd", "ids": ",".join(coin_ids),
                    "per_page": 250, "page": 1, "sparkline": "false"},
            headers=self._headers(),
            timeout=self.settings.http_timeout_s,
        )
        if not isinstance(data, list):
            raise ApiError(f"Réponse inattendue de CoinGecko /coins/markets: {data!r:.200}")
        return data

    def coin_community(self, coin_id: str) -> dict:
        """Données communautaires (followers, abonnés...) d'une crypto."""
        self._throttle()
        data = get_json(
            self.session,
            f"{self.settings.coingecko_base_url}/coins/{coin_id}",
            params={"localization": "false", "tickers": "false", "market_data": "false",
                    "community_data": "true", "developer_data": "false", "sparkline": "false"},
            headers=self._headers(),
            timeout=self.settings.http_timeout_s,
        )
        if not isinstance(data, dict):
            raise ApiError(f"Réponse inattendue de CoinGecko /coins/{coin_id}")
        return data

    def all_coin_ids(self) -> set[str]:
        self._throttle()
        data = get_json(self.session, f"{self.settings.coingecko_base_url}/coins/list",
                        headers=self._headers(), timeout=self.settings.http_timeout_s)
        return {c["id"] for c in data}
