from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .backend.routes import handle_api_get, handle_api_post, parse_request_query
from .config import load_config
from .storage import FeedbackRepository


@dataclass(slots=True)
class DashboardContext:
    repository: FeedbackRepository
    static_dir: Path
    default_date: date | None
    app_config: object


def run_dashboard(config_path: str, host: str = "127.0.0.1", port: int = 8787, report_date: date | None = None) -> int:
    config = load_config(config_path)
    repository = FeedbackRepository(config.database_path)
    static_dir = Path(__file__).resolve().parent / "web"
    context = DashboardContext(
        repository=repository,
        static_dir=static_dir,
        default_date=report_date,
        app_config=config,
    )

    class DashboardHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self.send_response(204)
                self._send_cors_headers()
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_error(405, "Method Not Allowed")

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._serve_api(parsed.path, parsed.query)
                return
            if parsed.path in {"/", "/index.html"}:
                self._serve_file("index.html", "text/html; charset=utf-8")
                return
            if parsed.path == "/video.html":
                self._serve_file("video.html", "text/html; charset=utf-8")
                return
            if parsed.path == "/styles.css":
                self._serve_file("styles.css", "text/css; charset=utf-8")
                return
            if parsed.path == "/app.js":
                self._serve_file("app.js", "application/javascript; charset=utf-8")
                return
            if parsed.path == "/video.js":
                self._serve_file("video.js", "application/javascript; charset=utf-8")
                return
            if parsed.path == "/runtime-config.js":
                self._serve_runtime_config()
                return
            self.send_error(404, "Not Found")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self._serve_api_post(parsed.path, parsed.query)
                return
            self.send_error(405, "Method Not Allowed")

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

        def _serve_api(self, path: str, raw_query: str) -> None:
            status, payload = handle_api_get(
                repository=context.repository,
                path=path,
                query=parse_request_query(raw_query),
                default_date=context.default_date,
                app_config=context.app_config,
            )
            self._json_response(payload, status_code=status, cors=True)

        def _serve_api_post(self, path: str, raw_query: str) -> None:
            payload = self._read_json_body()
            status, result = handle_api_post(
                repository=context.repository,
                path=path,
                query=parse_request_query(raw_query),
                payload=payload,
                default_date=context.default_date,
                app_config=context.app_config,
            )
            self._json_response(result, status_code=status, cors=True)

        def _send_cors_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")

        def _serve_file(self, filename: str, content_type: str) -> None:
            path = context.static_dir / filename
            if not path.exists():
                self.send_error(404, "Not Found")
                return
            payload = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _serve_runtime_config(self) -> None:
            js = "window.MEDIA_PULSE_CONFIG = {\"apiBaseUrl\": \"\"};\n"
            payload = js.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _json_response(self, payload: dict, status_code: int = 200, cors: bool = False) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            if cors:
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
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return {}
            if isinstance(data, dict):
                return data
            return {}

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard (fullstack) running at http://{host}:{port}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
