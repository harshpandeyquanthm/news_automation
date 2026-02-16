"""
News Fetcher - Main entry point.
Fetches news from Ticker Tape API and stores to MongoDB.
Can be run directly or via the scheduler.
"""
from datetime import datetime
from services.news_fetcher import fetch_news_from_tickertape, transform_article
from services.news_storage import store_news, log_fetch_run, get_last_stored_date
from db.Connection import close_connection


def run_fetch_job():
    """Single fetch cycle: fetch -> transform -> store -> log"""
    print(f"\n{'='*50}")
    print(f"[{datetime.now()}] Starting news fetch job...")
    print(f"{'='*50}")

    try:
        # 1. Check last stored date to know how far back to fetch
        last_date = get_last_stored_date()

        # 2. Fetch from API (paginates until caught up)
        raw_articles = fetch_news_from_tickertape(last_stored_date=last_date)

        if not raw_articles:
            log_fetch_run(0, 0, status="no_data")
            return

        # 2. Transform to our schema
        transformed = [transform_article(a) for a in raw_articles]

        # 3. Store to MongoDB (with dedup)
        new_count = store_news(transformed)

        # 4. Log the run
        log_fetch_run(len(raw_articles), new_count)

        print(f"[{datetime.now()}] Job complete. {new_count} new articles stored.")

    except Exception as e:
        print(f"[{datetime.now()}] Job failed: {e}")
        log_fetch_run(0, 0, status="error", error_msg=str(e))


if __name__ == "__main__":
    run_fetch_job()
    close_connection()
