"""Teste le retry sur un VRAI serveur HTTP local qui échoue volontairement."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from crypto_pipeline.clients.http import ApiError, build_session, get_json


@pytest.fixture()
def server():
    state = {"calls": 0, "script": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            code = state["script"][min(state["calls"], len(state["script"]) - 1)]
            state["calls"] += 1
            body = json.dumps({"ok": True}).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{httpd.server_port}/x"
    yield state
    httpd.shutdown()


def test_retries_on_503_then_succeeds(server):
    server["script"] = [503, 503, 200]
    session = build_session(total_retries=3, backoff=0.01)
    assert get_json(session, server["url"]) == {"ok": True}
    assert server["calls"] == 3                       # 2 échecs + 1 succès


def test_gives_up_after_max_retries(server):
    server["script"] = [500]
    session = build_session(total_retries=2, backoff=0.01)
    with pytest.raises(ApiError):
        get_json(session, server["url"])
    assert server["calls"] == 3                       # 1 essai + 2 retries


def test_no_retry_on_client_error(server):
    server["script"] = [404]
    session = build_session(total_retries=3, backoff=0.01)
    with pytest.raises(ApiError, match="404"):
        get_json(session, server["url"])
    assert server["calls"] == 1                       # une 404 ne s'arrange pas en réessayant


def test_451_gives_actionable_message(server):
    server["script"] = [451]
    with pytest.raises(ApiError, match="BINANCE_BASE_URL"):
        get_json(build_session(total_retries=0), server["url"])
