from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from ..config import load_config
from ..storage import FeedbackRepository
from .routes import handle_api_get, handle_api_post, parse_request_query


@dataclass(slots=True)
class BackendContext:
    repository: FeedbackRepository
    default_date: date | None
    app_config: object


def run_backend_server(
    config_path: str,
    host: str = "127.0.0.1",
    port: int = 8788,
    report_date: date | None = None,
) -> int:
    config = load_config(config_path)
    context = BackendContext(repository=FeedbackRepository(config.database_path), default_date=report_date, app_config=config)

    class BackendHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self._send_cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not parsed.path.startswith("/api/"):
                self._json_response({"error": "Not Found", "path": parsed.path}, status_code=404)
                return

            status, payload = handle_api_get(
                repository=context.repository,
                path=parsed.path,
                query=parse_request_query(parsed.query),
                default_date=context.default_date,
                app_config=context.app_config,
            )
            self._json_response(payload, status_code=status)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not parsed.path.startswith("/api/"):
                self._json_response({"error": "Not Found", "path": parsed.path}, status_code=404)
                return
            body = self._read_json_body()
            status, payload = handle_api_post(
                repository=context.repository,
                path=parsed.path,
                query=parse_request_query(parsed.query),
                payload=body,
                default_date=context.default_date,
                app_config=context.app_config,
            )
            self._json_response(payload, status_code=status)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

        def _send_cors_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")

        def _json_response(self, payload: dict, status_code: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = max(0, int(raw_length))
            except ValueError:
                length = 0
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return {}
            if isinstance(payload, dict):
                return payload
            return {}

    server = ThreadingHTTPServer((host, port), BackendHandler)
    print(f"Backend API running at http://{host}:{port}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
