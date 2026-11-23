"""Client Binance (endpoint public /api/v3/klines, sans clé API)."""
from __future__ import annotations

import requests

from crypto_pipeline.clients.http import ApiError, build_session, get_json
from crypto_pipeline.config import get_settings


class BinanceClient:
    def __init__(self, session: requests.Session | None = None):
        self.settings = get_settings()
        self.session = session or build_session()

    def klines(self, binance_symbol: str, *, interval: str = "1m", limit: int = 5,
               start_ms: int | None = None, end_ms: int | None = None) -> list[list]:
        """Retourne les bougies brutes : liste de tableaux (voir transform.parse_klines)."""
        params: dict = {"symbol": binance_symbol, "interval": interval, "limit": limit}
        if start_ms is not None:
            params["startTime"] = start_ms
        if end_ms is not None:
            params["endTime"] = end_ms
        data = get_json(
            self.session,
            f"{self.settings.binance_base_url}/api/v3/klines",
            params=params,
            timeout=self.settings.http_timeout_s,
        )
        if not isinstance(data, list):
            raise ApiError(f"Réponse inattendue de Binance pour {binance_symbol}: {data!r:.200}")
        return data

    def exchange_symbols(self) -> set[str]:
        """Toutes les paires actuellement cotées (utile pour valider coins.yaml)."""
        data = get_json(self.session, f"{self.settings.binance_base_url}/api/v3/exchangeInfo",
                        timeout=self.settings.http_timeout_s)
        return {s["symbol"] for s in data["symbols"] if s.get("status") == "TRADING"}
