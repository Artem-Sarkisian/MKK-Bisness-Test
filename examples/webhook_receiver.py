"""Local webhook receiver: /success, /fail, /flaky, /slow; GET /events."""

import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Receiver(BaseHTTPRequestHandler):
    events: list[dict] = []
    attempts: Counter = Counter()
    lock = threading.Lock()

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 65536:
            self.send_error(413)
            return
        payload = json.loads(self.rfile.read(length))
        with self.lock:
            event_id = payload.get("event_id")
            self.attempts[event_id] += 1
            attempt = self.attempts[event_id]
            self.events.append(
                {
                    "received_at": time.time(),
                    "path": self.path,
                    "body": payload,
                }
            )
        status = (
            503
            if self.path == "/fail" or (self.path == "/flaky" and attempt < 3)
            else 204
        )
        if self.path == "/slow":
            time.sleep(10)
        self.send_response(status)
        self.end_headers()

    def do_GET(self) -> None:
        if self.path != "/events":
            self.send_error(404)
            return
        with self.lock:
            body = json.dumps(self.events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Receiver).serve_forever()
