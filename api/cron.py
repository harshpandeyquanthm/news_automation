import os
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler

# Add project root to path so local imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.news_fetcher import fetch_news_from_tickertape, transform_article
from services.news_storage import store_news, log_fetch_run, get_last_stored_date
from db.Connection import close_connection


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        auth_header = self.headers.get("Authorization")
        cron_secret = os.getenv("CRON_SECRET")
        if cron_secret and auth_header != f"Bearer {cron_secret}":
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Unauthorized"}).encode())
            return

        try:
            last_date = get_last_stored_date()
            raw_articles = fetch_news_from_tickertape(last_stored_date=last_date)

            if not raw_articles:
                log_fetch_run(0, 0, status="no_data")
                self._respond(200, {
                    "status": "no_data",
                    "message": "No new articles found",
                    "timestamp": datetime.now().isoformat(),
                })
                return

            transformed = [transform_article(a) for a in raw_articles]
            new_count = store_news(transformed)
            log_fetch_run(len(raw_articles), new_count)

            self._respond(200, {
                "status": "success",
                "fetched": len(raw_articles),
                "new_articles": new_count,
                "timestamp": datetime.now().isoformat(),
            })

        except Exception as e:
            log_fetch_run(0, 0, status="error", error_msg=str(e))
            self._respond(500, {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            })

        finally:
            close_connection()

    def _respond(self, status_code, body):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())
