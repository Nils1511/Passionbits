def extract_ad_videos(ad_json: dict) -> str:
    """
    Traverse ad_json["snapshot"]["cards"] and
    return a space‑separated string of all videoHdUrl values.
    """
    cards = ad_json.get("snapshot", {}).get("cards", [])
    urls = [c.get("videoHdUrl", "") for c in cards if c.get("videoHdUrl")]
    return " ".join(urls)

def extract_reel_comments_texts(cur, reel_db_id: int) -> str:
    """
    Query reel_comments for that reel_id, then join all comment texts.
    """
    cur.execute("""
        SELECT text
          FROM reel_comments
         WHERE reel_id = %s
    """, (reel_db_id,))
    return " ".join(r[0] for r in cur.fetchall() if r[0])

import psycopg2, json, re, time, backoff
from ratelimit import limits, sleep_and_retry
from google import genai
from google.genai.errors import ClientError

# … same config, regex_filter, gemini_filter as before …

def process_ads_table(table_name: str, keywords: list):
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()
    # fetch raw_json and id
    cur.execute(f"SELECT id, raw_json FROM {table_name} WHERE is_relevant IS NULL;")
    rows = cur.fetchall()

    updates = []
    for db_id, raw in rows:
        ad = json.loads(raw)
        # build prompt text: caption + all videoHdUrl
        caption = ad.get("snapshot", {}).get("caption", "")
        video_urls = extract_ad_videos(ad)
        text = f"Caption: {caption}\nVideo URLs: {video_urls}"

        # filter
        if regex_filter(text, keywords):
            relevant = True
        else:
            relevant = gemini_filter(text, keywords)

        updates.append((relevant, db_id))

    cur.executemany(f"UPDATE {table_name} SET is_relevant=%s WHERE id=%s;", updates)
    conn.commit()
    cur.close()
    conn.close()


def process_reels_table(table_name: str, comments_table: str, keywords: list):
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()
    # fetch reels
    cur.execute(f"SELECT id, raw_json, video_url, display_url FROM {table_name} WHERE is_relevant IS NULL;")
    rows = cur.fetchall()

    updates = []
    for db_id, raw, video_url, display_url in rows:
        reel = json.loads(raw)
        # get comments text
        comments_text = extract_reel_comments_texts(cur, db_id)
        text = f"Caption: {reel.get('caption','')}\n" \
               f"Video URL: {video_url}\n" \
               f"Display URL: {display_url}\n" \
               f"Comments: {comments_text}"

        if regex_filter(text, keywords):
            relevant = True
        else:
            relevant = gemini_filter(text, keywords)

        updates.append((relevant, db_id))

    cur.executemany(f"UPDATE {table_name} SET is_relevant=%s WHERE id=%s;", updates)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    raw = input("Enter comma‑separated keywords: ").strip()
    keywords = [w.strip() for w in raw.split(",") if w.strip()]

    print("Filtering ads…")
    process_ads_table("competitor_ads", keywords)

    print("Filtering reels…")
    process_reels_table("competitor_reels", "reel_comments", keywords)

    print("Done.")



#!/usr/bin/env python3
import os
import re
import json
import time
import backoff
import psycopg2
from dotenv import load_dotenv
from google import genai
from google.genai.errors import ClientError
from ratelimit import limits, sleep_and_retry

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY     = os.getenv('GEMINI_API_KEY')
MODEL_NAME  = "gemini-2.0-flash-001"
DEVICE      = "cuda" if os.getenv("USE_CUDA","false").lower()=="true" else "cpu"

PG_CONN     = {
    "host":   os.getenv("PG_HOST"),
    "port":   os.getenv("PG_PORT","5432"),
    "dbname": os.getenv("PG_DB"),
    "user":   os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
}

# rate‑limit Gemini
_ONE_MINUTE = 60
MAX_RETRIES = 5
RETRY_DELAY = 5

# instantiate Gemini
client = genai.Client(api_key=API_KEY)

# ─── 1. TEXT FILTER: simple regex ────────────────────────────────────────────────
def regex_filter(text: str, keywords: list) -> bool:
    """
    return True if any keyword appears (word‑boundary, case‑insensitive)
    """
    t = (text or "").lower()
    for kw in keywords:
        if re.search(rf'\b{re.escape(kw.lower())}\b', t):
            return True
    return False

# ─── 2. TEXT FILTER: Gemini zero-shot ──────────────────────────────────────────
@sleep_and_retry
@limits(calls=15, period=_ONE_MINUTE)
@backoff.on_exception(
    backoff.expo,
    ClientError,
    max_time=60,
    giveup=lambda e: not getattr(e, "status_code", None) in (429, 503)
)
def gemini_filter(text: str, keywords: list) -> bool:
    """
    Ask Gemini: "Does this content feature <keywords>?"
    """
    joined = ", ".join(keywords)
    prompt = (
        "You are a yes/no classifier.  Reply with exactly 'Yes' or 'No'.\n\n"
        f"Question: Does the following social‑media post feature {joined}?\n"
        f"Content: {text}\n"
        "Answer 'Yes' or 'No'."
    )

    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt
            )
            ans = resp.text.strip().lower()
            return ans.startswith("yes")
        except ClientError as e:
            code = getattr(e, "status_code", None)
            if code in (429, 503) and attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt
                time.sleep(wait)
                continue
            raise

    return False

# ─── 3. Fetch rows, apply filters, update DB ──────────────────────────────────
def process_table(table_name: str, text_path: list, keywords: list):
    """
    text_path: list of JSON keys to extract the text field, e.g. ["caption"] or ["snapshot","caption"]
    """
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()

    # 1) fetch all rows where is_relevant IS NULL
    cur.execute(f"SELECT id, raw_json FROM {table_name} WHERE is_relevant IS NULL")
    rows = cur.fetchall()

    updates = []
    for db_id, raw in rows:
        data = json.loads(raw)
        # drill into text fields
        texts = []
        def extract(d, path):
            for k in path:
                d = d.get(k, {})
            return d or ""
        # if multiple text paths, combine them
        for p in text_path:
            texts.append(extract(data, p) if isinstance(p, list) else extract(data, p))
        full_text = " ".join(texts)

        # step 1: regex
        if regex_filter(full_text, keywords):
            relevant = True
        else:
            # step 2: Gemini
            relevant = gemini_filter(full_text, keywords)

        updates.append((relevant, db_id))

    # 2) batch UPDATE
    execute_sql = f"UPDATE {table_name} SET is_relevant = %s WHERE id = %s"
    cur.executemany(execute_sql, updates)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    # 1) read user query terms, e.g. ["t-shirt","polo"]
    raw = input("Enter comma‑separated keywords to filter by: ").strip()
    keywords = [w.strip() for w in raw.split(",") if w.strip()]

    # 2) Apply to both tables:
    #    – ads: perhaps text at raw_json → snapshot → caption
    #    – reels: text at raw_json → caption
    process_table("competitor_ads", text_path=[["snapshot","caption"]], keywords=keywords)
    process_table("reels",          text_path=[["caption"]],             keywords=keywords)

    print("Filtering complete.")
