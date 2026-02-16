import requests
from datetime import datetime

BASE_URL = "https://analyze.api.tickertape.in/v2/homepage/events"
PAGE_SIZE = 10           # articles per API call (matches API's lazy-load design)
MAX_PAGES = 500          # safety limit to avoid infinite loops


def fetch_news_from_tickertape(last_stored_date=None):
    """
    Fetches news from Ticker Tape API with pagination.
    - If last_stored_date is provided, keeps paginating until we reach
      articles older than that date (catches up after downtime).
    - If last_stored_date is None (first run), fetches only the first page.
    Returns list of news articles or empty list on failure.
    """
    headers = {"Accept": "application/json"}
    all_articles = []

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        params = {
            "count": PAGE_SIZE,
            "offset": offset,
            "sids": "",
            "type": "news",
        }

        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            articles = data["data"]["news"]
        except requests.exceptions.RequestException as e:
            print(f"[{datetime.now()}] Error fetching page {page}: {e}")
            break

        if not articles:
            print(f"[{datetime.now()}] No more articles at offset {offset}, stopping.")
            break

        all_articles.extend(articles)
        print(f"[{datetime.now()}] Page {page + 1}: fetched {len(articles)} articles (offset {offset})")

        # Check if we've caught up to what's already in the DB
        if last_stored_date:
            oldest_in_batch = min(a.get("date", "") for a in articles)
            if oldest_in_batch <= last_stored_date:
                print(f"[{datetime.now()}] Reached already-stored data (oldest in batch: {oldest_in_batch}). Stopping.")
                break
        else:
            # First run — just fetch one page
            break

    print(f"[{datetime.now()}] Total fetched: {len(all_articles)} articles across {page + 1} page(s)")
    return all_articles


def transform_article(raw_article):
    """
    Transforms raw API response into our DB schema.
    PSEUDO: Adjust field mappings based on actual API response fields.
    """
    return {
        "headline": raw_article.get("headline", ""),      
        "summary": raw_article.get("summary", ""),                            
        "date": raw_article.get("date", ""),                         
        "publisher": raw_article.get("publisher", ""),                         
        "stocks": raw_article.get("stocks", []), 
        "tag" : raw_article.get("tag",""),
        "fetched_at": datetime.now(),
    }
