from __future__ import annotations

import json
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


@dataclass(slots=True)
class FrontendContext:
    static_dir: Path
    api_base_url: str


def run_frontend_server(
    host: str = "127.0.0.1",
    port: int = 8787,
    api_base_url: str = "http://127.0.0.1:8788",
) -> int:
    static_dir = Path(__file__).resolve().parents[1] / "web"
    context = FrontendContext(static_dir=static_dir, api_base_url=api_base_url.rstrip("/"))

    class FrontendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
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

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

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
            config = {"apiBaseUrl": context.api_base_url}
            js = f"window.NT_CAM_PULSE_CONFIG = {json.dumps(config, ensure_ascii=False)};\n"
            payload = js.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer((host, port), FrontendHandler)
    print(f"Frontend running at http://{host}:{port}")
    print(f"Using API base: {context.api_base_url}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
