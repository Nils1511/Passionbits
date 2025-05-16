#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import execute_values
from apify_client import ApifyClient
from dotenv import load_dotenv

from competitors_name import get_competitors

# ——— Logging ——————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ——— Load configuration ————————————————————————————————
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_API_KEY")
APIFY_ACTOR_ID = "JJghSZmShuco4j9gJ"
PG_CONN_PARAMS = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT", 5432),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
}

apify_client = ApifyClient(APIFY_TOKEN)

# ——— Scrape one brand ——————————————————————————————————————————————
def scrape_ads_for_brand(brand_tuple, results_limit=50, start_date=None, end_date=None):
    brand, fb_url = brand_tuple
    if fb_url == "N/A":
        return []
    run_input = {
        "startUrls": [{"url": fb_url}],
        "resultsLimit": results_limit,
        "activeStatus": "active",
        "scrapeAdDetails": True,
        "mediaType": "VIDEO",
    }
    if start_date: run_input["startDate"] = start_date
    if end_date:   run_input["endDate"]   = end_date

    run = apify_client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
    ads = []
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        item["brand"] = brand
        ads.append(item)
    logger.info(f"[{brand}] fetched {len(ads)} ads")
    return ads

# ——— Normalize competitor_ads ———————————————————————————————————————
def normalize_ad(ad):
    """Return a tuple matching competitor_ads columns (minus id, inserted_at)."""
    start_ts = ad.get("startDate")
    end_ts   = ad.get("endDate")
    page_info = ad.get("pageInfo", {}) \
                  .get("adLibraryPageInfo", {}) \
                  .get("pageInfo", {})
    snapshot = ad.get("snapshot", {})

    return (
        ad["brand"],
        ad.get("inputUrl"),
        page_info.get("pageId"),
        page_info.get("pageName"),
        page_info.get("likes"),
        ad.get("adArchiveID") or ad.get("adArchiveId"),
        datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts else None,
        datetime.fromtimestamp(end_ts, tz=timezone.utc)   if end_ts   else None,
        ad.get("isActive"),
        ad.get("totalActiveTime"),
        snapshot.get("ctaText"),
        snapshot.get("linkUrl"),
        snapshot.get("caption"),
        json.dumps(ad)  # raw_json
    )

# ——— Extract cards per ad ——————————————————————————————————————————
def extract_cards(ad, ad_db_id):
    """
    Given the raw ad dict and the DB primary key for competitor_ads,
    produce a list of tuples matching ad_cards columns (minus id, inserted_at).
    """
    cards = ad.get("snapshot", {}).get("cards", [])
    logger.debug(f"Ad {ad.get('adArchiveID')} has {len(cards)} cards")
    out = []
    for c in cards:
        body = c.get("body")
        body_value = body.get("text") if isinstance(body, dict) else str(body) if body else None
        out.append((
            ad_db_id,
            body_value,
            c.get("caption"),
            c.get("ctaText"),
            c.get("ctaType"),
            c.get("linkDescription"),
            c.get("linkUrl"),
            c.get("title"),
            c.get("videoHdUrl"),
            c.get("videoSdUrl"),
            c.get("videoPreviewImageUrl")
        ))
    return out

# ——— Bulk insert into both tables —————————————————————————————————————
def save_all(ads):
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    cur = conn.cursor()

    # 1) competitor_ads
    ad_rows = [normalize_ad(ad) for ad in ads]
    insert_ads_sql = """
        INSERT INTO competitor_ads
          (brand, input_url, page_id, page_name, page_likes,
           ad_archive_id, start_date, end_date, is_active,
           total_active_time, cta_text, link_url,
           snapshot_caption, raw_json)
        VALUES %s
        RETURNING id
    """
    # execute and collect generated IDs
    ad_db_ids = execute_values(cur, insert_ads_sql, ad_rows, fetch=True)
    ad_db_ids = [r[0] for r in ad_db_ids]


    # 2) ad_cards
    card_rows = []
    for ad, db_id in zip(ads, ad_db_ids):
        card_rows.extend(extract_cards(ad, db_id))

    if card_rows:
        insert_cards_sql = """
            INSERT INTO ad_cards
              (ad_id, body, caption, cta_text, cta_type,
               link_description, link_url, title,
               video_hd_url, video_sd_url, video_preview_image)
            VALUES %s
        """
        execute_values(cur, insert_cards_sql, card_rows)

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Inserted {len(ad_rows)} ads and {len(card_rows)} cards")

# ——— Main ——————————————————————————————————————————————
if __name__ == "__main__":
    product = input("Product Name: ").strip()
    competitors = [
        ('Ralph Lauren', 'https://www.facebook.com/RalphLauren'),
        ] #get_competitors(product)

    # 1) Parallel scrape
    all_ads = []
    with ThreadPoolExecutor(max_workers=8) as exe:
        futures = {exe.submit(scrape_ads_for_brand, c, 15, "2024-01-01", "2024-04-30"): c for c in competitors}
        for fut in as_completed(futures):
            all_ads.extend(fut.result())

    logger.info(f"Total ads scraped: {len(all_ads)}")

    # 2) Save both ads + cards
    save_all(all_ads)
