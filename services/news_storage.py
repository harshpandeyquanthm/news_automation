from datetime import datetime
from db.Connection import get_collection, Collections


def get_last_stored_date():
    """
    Returns the date string of the most recent article in MongoDB.
    Returns None if no articles exist (first run).
    """
    collection = get_collection(Collections.ARTICLES)
    latest = collection.find_one(
        {"date": {"$exists": True, "$ne": ""}},
        sort=[("date", -1)],
        projection={"date": 1}
    )
    if latest:
        print(f"[{datetime.now()}] Last stored article date: {latest['date']}")
        return latest["date"]
    print(f"[{datetime.now()}] No articles in DB yet (first run)")
    return None


def store_news(articles):
    """
    Stores news articles to MongoDB, skipping duplicates.
    Deduplication is based on article URL (or title if URL not available).
    Returns count of newly inserted articles.
    """
    if not articles:
        print(f"[{datetime.now()}] No articles to store")
        return 0

    collection = get_collection(Collections.ARTICLES)  # PSEUDO: replace collection name if needed
    inserted_count = 0

    for article in articles:
        # PSEUDO: Change the dedup field to match your needs (url, article_title, etc.)
        dedup_filter = {"url": article.get("url")}

        existing = collection.find_one(dedup_filter)

        if existing is None:
            article["stored_at"] = datetime.now()
            collection.insert_one(article)
            inserted_count += 1

    print(f"[{datetime.now()}] Inserted {inserted_count} new articles (skipped {len(articles) - inserted_count} duplicates)")
    return inserted_count


def log_fetch_run(total_fetched, newly_inserted, status="success", error_msg=None):
    """
    Logs each fetch run for monitoring.
    """
    log_collection = get_collection(Collections.SCRAPE_LOGS)
    log_entry = {
        "timestamp": datetime.now(),
        "total_fetched": total_fetched,
        "newly_inserted": newly_inserted,
        "status": status,
        "error": error_msg,
    }
    log_collection.insert_one(log_entry)
