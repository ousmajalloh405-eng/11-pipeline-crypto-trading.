#!/usr/bin/env python3
"""Faux serveur Binance (/api/v3/klines) pour développer et tester SANS internet.

    python scripts/mock_binance.py            # écoute sur http://127.0.0.1:9001
    BINANCE_BASE_URL=http://127.0.0.1:9001 python -m crypto_pipeline.cli ingest-klines

Génère des bougies déterministes (marche aléatoire seedée par symbole + minute),
au même format que l'API réelle, en respectant limit / startTime / endTime.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

MINUTE = 60_000


def _price(symbol: str, minute_idx: int) -> float:
    base = 10 + int(hashlib.md5(symbol.encode()).hexdigest()[:4], 16) % 5000
    h = int(hashlib.md5(f"{symbol}{minute_idx}".encode()).hexdigest()[:6], 16)
    return round(base * (1 + ((h % 2001) - 1000) / 100_000), 6)


def make_kline(symbol: str, open_ms: int) -> list:
    idx = open_ms // MINUTE
    o, c = _price(symbol, idx), _price(symbol, idx + 1)
    hi, lo = max(o, c) * 1.0005, min(o, c) * 0.9995
    vol = round(1 + (idx % 97) / 10, 4)
    return [open_ms, f"{o}", f"{hi:.6f}", f"{lo:.6f}", f"{c}", f"{vol}",
            open_ms + MINUTE - 1, f"{vol * c:.4f}", 10 + idx % 50, "0", "0", "0"]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path != "/api/v3/klines" or "symbol" not in q:
            return self._send(404, {"msg": "not found"})
        limit = min(int(q.get("limit", 500)), 1000)
        now = int(time.time() * 1000)
        end = min(int(q.get("endTime", now)), now)
        if "startTime" in q:
            first = (int(q["startTime"]) // MINUTE) * MINUTE
        else:
            first = (end // MINUTE) * MINUTE - (limit - 1) * MINUTE
        out, t = [], first
        while t <= end and len(out) < limit:
            out.append(make_kline(q["symbol"], t))
            t += MINUTE
        self._send(200, out)

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    print(f"Mock Binance sur http://127.0.0.1:{port}")
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
