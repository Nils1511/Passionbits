#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import execute_values
from competitors_name import get_competitors
from apify_client import ApifyClient
from dotenv import load_dotenv

# ——— Logging ——————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ——— Load configuration ————————————————————————————————
load_dotenv()
APIFY_TOKEN    = os.getenv("APIFY_API_KEY")
FB_ACTOR_ID    = "JJghSZmShuco4j9gJ"
IG_ACTOR_ID    = "xMc5Ga1oCONPmWJIa"
PG_CONN_PARAMS = {
    "host":     os.getenv("PG_HOST"),
    "port":     os.getenv("PG_PORT", 5432),
    "dbname":   os.getenv("PG_DB"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
}

apify = ApifyClient(APIFY_TOKEN)


# ——— Scrape Facebook ads for one brand ——————————————————————
def scrape_ads_for_brand(brand, fb_url, results_limit=50, start_date=None, end_date=None):
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

    run = apify.actor(FB_ACTOR_ID).call(run_input=run_input)
    ads = []
    for item in apify.dataset(run["defaultDatasetId"]).iterate_items():
        item["brand"] = brand
        ads.append(item)
    logger.info(f"[FB:{brand}] fetched {len(ads)} ads")
    print(json.dumps(ads[0], indent=2))
    return ads

def normalize_ad(ad):
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
        json.dumps(ad),
    )

def extract_ad_cards(ad, ad_db_id):
    snapshot = ad.get("snapshot", {})
    cards = snapshot.get("cards", [])
    out = []
    for c in cards:
        body = c.get("body")
        body_value = body.get("text") if isinstance(body, dict) else (str(body) if body else None)
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
            c.get("videoPreviewImageUrl"),
        ))
    videos = snapshot.get("videos", [])
    for v in videos:
        out.append((
            ad_db_id,
            None,  # body
            None,  # caption
            None,  # cta_text
            None,  # cta_type
            None,  # link_description
            None,  # link_url
            None,  # title
            v.get("videoHdUrl"),
            v.get("videoSdUrl"),
            v.get("videoPreviewImageUrl"),
        ))
    return out


# ——— Scrape Instagram reels for one brand —————————————————————
def scrape_reels_for_brand(brand, ig_username, results_limit=100):
    run_input = {
        "username": [ig_username],
        "resultsLimit": results_limit,
    }
    run = apify.actor(IG_ACTOR_ID).call(run_input=run_input)
    reels = []
    for item in apify.dataset(run["defaultDatasetId"]).iterate_items():
        item["brand"] = brand
        reels.append(item)
    logger.info(f"[IG:{brand}] fetched {len(reels)} reels")
    return reels

def normalize_reel(r):
    ts = None
    if r.get("timestamp"):
        ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
    return (
        r["brand"],
        r.get("inputUrl"),
        r.get("id"),
        r.get("shortCode"),
        r.get("caption"),
        r.get("url"),
        r.get("commentsCount"),
        r.get("likesCount"),
        r.get("videoUrl"),
        r.get("displayUrl"),
        ts,
        json.dumps(r),
    )

def extract_reel_comments(r, reel_db_id):
    comments = r.get("latestComments", [])
    out = []
    for c in comments:
        out.append((
            reel_db_id,
            c.get("id"),
            c.get("text"),
            c.get("ownerUsername"),
            c.get("owner", {}).get("id"),
            datetime.fromisoformat(c["timestamp"].replace("Z", "+00:00")) if c.get("timestamp") else None,
            None,          # no parent_comment_id for top‐level
            json.dumps(c),
        ))
        # also store any replies
        for reply in c.get("replies", []):
            out.append((
                reel_db_id,
                reply.get("id"),
                reply.get("text"),
                reply.get("ownerUsername"),
                reply.get("owner", {}).get("id"),
                datetime.fromisoformat(reply["timestamp"].replace("Z", "+00:00")) if reply.get("timestamp") else None,
                c.get("id"),  # parent
                json.dumps(reply),
            ))
    return out


# ——— Bulk‐insert into Postgres ——————————————————————————————
def save_ads(conn, ads):
    cur = conn.cursor()
    # cur.execute("TRUNCATE TABLE ad_cards RESTART IDENTITY CASCADE;")
    # cur.execute("TRUNCATE TABLE competitor_ads RESTART IDENTITY CASCADE;")
    ad_rows = [normalize_ad(a) for a in ads]
    sql_ads = """
      INSERT INTO competitor_ads
        (brand, input_url, page_id, page_name, page_likes,
         ad_archive_id, start_date, end_date, is_active,
         total_active_time, cta_text, link_url,
         snapshot_caption, raw_json)
      VALUES %s RETURNING id
    """
    ad_ids = execute_values(cur, sql_ads, ad_rows, fetch=True)
    ad_ids = [r[0] for r in ad_ids]

    card_rows = []
    for ad, aid in zip(ads, ad_ids):
        card_rows += extract_ad_cards(ad, aid)

    if card_rows:
        sql_cards = """
          INSERT INTO ad_cards
            (ad_id, body, caption, cta_text, cta_type,
             link_description, link_url, title,
             video_hd_url, video_sd_url, video_preview_image)
          VALUES %s
        """
        execute_values(cur, sql_cards, card_rows)
    conn.commit()
    logger.info(f"Inserted {len(ad_rows)} ads + {len(card_rows)} cards")
    cur.close()


def save_reels(conn, reels):
    cur = conn.cursor()
    # cur.execute("TRUNCATE TABLE reel_comments RESTART IDENTITY CASCADE;")
    # cur.execute("TRUNCATE TABLE competitor_reels RESTART IDENTITY CASCADE;")
    reel_rows = [normalize_reel(r) for r in reels]
    sql_reels = """
      INSERT INTO competitor_reels
        (brand, input_url, reel_id, shortcode, caption,
         url, comments_count, likes_count,
         video_url, display_url, timestamp, raw_json)
      VALUES %s RETURNING id
    """
    reel_ids = execute_values(cur, sql_reels, reel_rows, fetch=True)
    reel_ids = [r[0] for r in reel_ids]

    comment_rows = []
    for reel, rid in zip(reels, reel_ids):
        comment_rows += extract_reel_comments(reel, rid)

    if comment_rows:
        sql_comments = """
          INSERT INTO reel_comments
            (reel_id, comment_id, text, owner_username, owner_id,
             timestamp, parent_comment_id, raw_json)
          VALUES %s
        """
        execute_values(cur, sql_comments, comment_rows)
    conn.commit()
    logger.info(f"Inserted {len(reel_rows)} reels + {len(comment_rows)} comments")
    cur.close()


# ——— Main ——————————————————————————————————————————————
if __name__ == "__main__":
    # q = input("Enter product or brand query: ").strip()
    # competitors = get_competitors(q, top_k=3) 
    # print("competitors", competitors)
    # Input: only the competitor brand & their Facebook URL *or* Insta username
    competitors = [
        ('Snitch', 'https://www.facebook.com/snitch.co.in', 'snitch.co.in'), 
        ('Bewakoof', 'https://www.facebook.com/bewakoof', 'bewakoofofficial'),
    #     # add more: ("Brand", "https://facebook...", "instagram_username")
    ]

    # scrape in parallel
    all_fb_ads   = []
    all_ig_reels = []
    with ThreadPoolExecutor(max_workers=8) as exe:
        futures = {}
        for brand, fb_url, ig_username in competitors:


            # Skip if both are empty or N/A
            if (not fb_url or fb_url == 'N/A') and (not ig_username or ig_username == 'N/A'):
                continue
            if fb_url and fb_url != 'N/A' and fb_url.startswith("http"):
                futures[exe.submit(scrape_ads_for_brand, brand, fb_url, 50, "2024-01-01", "2024-04-30")] = "ads"
            if ig_username and ig_username != 'N/A':
                futures[exe.submit(scrape_reels_for_brand, brand, ig_username, 50)] = "reels"

        for fut in as_completed(futures):
            kind = futures[fut]
            try:
                data = fut.result()
                if kind == "ads":
                    all_fb_ads.extend(data)
                else:
                    all_ig_reels.extend(data)
            except Exception as e:
                logger.error(f"Error scraping {kind}: {e}")

    logger.info(f"Total FB ads: {len(all_fb_ads)}, Total IG reels: {len(all_ig_reels)}")

    # save into Postgres
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    print("About to save ads")
    save_ads(conn, all_fb_ads)
    print("Saved ads")
    print("About to save reels")
    save_reels(conn, all_ig_reels)
    print("Saved reels")
    conn.close()
