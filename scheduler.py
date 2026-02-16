"""
Scheduler - Runs the news fetch job every 30 minutes.
Uses APScheduler for in-process scheduling.
"""
import os
import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from main import run_fetch_job
from db.Connection import close_connection

load_dotenv()


def graceful_shutdown(signum, frame):
    print("\nShutting down scheduler...")
    close_connection()
    sys.exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

if __name__ == "__main__":
    interval_minutes = int(os.getenv("FETCH_INTERVAL_MINUTES", 30))  # PSEUDO: change interval if needed

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_fetch_job,
        trigger="interval",
        minutes=interval_minutes,
        id="news_fetch_job",
        name="Fetch news from Ticker Tape",
        max_instances=1,
    )

    # Run once immediately on startup
    print(f"Running initial fetch...")
    run_fetch_job()

    print(f"\nScheduler started. Fetching every {interval_minutes} minutes.")
    print("Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        graceful_shutdown(None, None)
