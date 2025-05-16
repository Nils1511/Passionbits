import os
import requests
from urllib.parse import urlencode
from apify_client import ApifyClient
import json
from competitors_name import get_competitors
from dotenv import load_dotenv
import time
import logging
import re
from google import genai

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# —————— Configuration ——————
GRAPH_API_TOKEN = os.getenv("FB_GRAPH_TOKEN")      # App or Page token
APIFY_TOKEN = os.getenv("APIFY_API_KEY")         # Your Apify API token
APIFY_ACTOR_ID = "JJghSZmShuco4j9gJ"               # Ads Library Actor ID
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")  # Add this to your .env

# Check if API tokens are available
if not GRAPH_API_TOKEN:
    logger.warning("FB_GRAPH_TOKEN not found in .env file. Some features may not work.")
    
if not APIFY_TOKEN:
    logger.warning("APIFY_API_TOKEN not found in .env file. Some features may not work.")
else:
    client = ApifyClient(APIFY_TOKEN)               # ApifyClient init


def scrape_ads_for_competitors(competitors, results_limit=50, start_date=None, end_date=None):
    all_ads = []
    for brand, fb_url in competitors:
        if fb_url == 'N/A':
            logger.info(f"Skipping '{brand}' (no Facebook page)")
            continue
        logger.info(f"Scraping ads for {brand}: {fb_url}")
        run_input = {
            "startUrls": [{"url": fb_url}],
            "resultsLimit": results_limit,
            "activeStatus": "active",
            "scrapeAdDetails": True,
            "mediaType": "VIDEO",  # Only video ads
        }
        if start_date:
            run_input["startDate"] = start_date
        if end_date:
            run_input["endDate"] = end_date

        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
        logger.info(f"Run finished for {brand}, fetching results…")
        for ad in client.dataset(run["defaultDatasetId"]).iterate_items():
            ad["brand"] = brand  # Tag ad with brand for clarity
            all_ads.append(ad)
    return all_ads

if __name__ == "__main__":
    # Example input: list of (brand_name, facebook_url) tuples
    # q = input("Enter the Product Name: ")
    competitors = [
        ('Ralph Lauren', 'https://www.facebook.com/RalphLauren'),
        ] #get_competitors(q)
    # Scrape ads using Apify
    ads = scrape_ads_for_competitors(
        competitors,
        results_limit=50,
        start_date="2024-01-01",
        end_date="2024-04-30"
    )

    # Save to JSON file
    output_path = "ralph_lauren_ads.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ads, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(ads)} ad records to '{output_path}'")
