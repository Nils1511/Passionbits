# from apify_client import ApifyClient
# import urllib.parse
# import json
# import os
# from competitors_name import get_competitors
# import numpy as np
# from dotenv import load_dotenv

# load_dotenv()

# API_TOKEN = os.getenv('APIFY_API_KEY')
# # Initialize the ApifyClient with your API token
# client = ApifyClient(API_TOKEN)

# def build_ads_library_url(competitor_name: str) -> str:
#     """
#     Constructs a Facebook Ads Library URL for a given competitor name.
#     """
#     base = "https://www.facebook.com/ads/library/"
#     params = {
#         "active_status": "active",
#         "ad_type":       "all",
#         "country":       "ALL",
#         "is_targeted_country": "false",
#         "media_type":    "all",
#         "search_type":   "keyword_unordered",
#         "q":             competitor_name,
#     }
#     return f"{base}?{urllib.parse.urlencode(params)}"

# def scrape_competitor_ads_by_name(
#     competitor_names: list[str],
#     max_items: int = 100,
# ) -> list[dict]:
#     """
#     Given competitor page names, build Ads Library URLs and scrape ads for each.
#     Returns a flat list of ad records.
#     """
#     urls = [{"url": build_ads_library_url(name)} for name in competitor_names]
#     print('URL:', urls)
#     run_input = {
#         "urls": urls,
#         "count": max_items,
#         "scrapePageAds.activeStatus": "all",
#         "period": "",
#     }
#     run = client.actor("XtaWFhbtfxyzqrFmd").call(run_input=run_input)

#     ads = []
#     # for item in client.dataset(run["defaultDatasetId"]).iterate_items():
#     #     ads.append(item)
#     return ads

# if __name__ == "__main__":
#     # Replace with your actual competitor names

#     q = input("Enter product or brand query: ").strip()
#     # Example with price filter from $20 to $50
#     competitors = get_competitors(q, top_k=20)   #, price_min=20.0, price_max=50.0)
#     print(f"Top competitors for '{q}' in :")

#     print(competitors)
#     # Scrape ads
#     ads = scrape_competitor_ads_by_name(competitors, max_items=15)

#     # Save to JSON file
#     output_path = "meta_profile_ads.json"
#     with open(output_path, "w", encoding="utf-8") as f:
#         json.dump(ads, f, ensure_ascii=False, indent=2)

#     print(f"Saved {len(ads)} ad records to '{output_path}'")

import os
import requests
from urllib.parse import urlencode
from apify_client import ApifyClient
import json
from competitors_name import get_competitors
from dotenv import load_dotenv
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# —————— Configuration ——————
GRAPH_API_TOKEN = os.getenv("FB_GRAPH_TOKEN")      # App or Page token
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")         # Your Apify API token
APIFY_ACTOR_ID = "XtaWFhbtfxyzqrFmd"               # Ads Library Actor ID

# Check if API tokens are available
if not GRAPH_API_TOKEN:
    logger.warning("FB_GRAPH_TOKEN not found in .env file. Some features may not work.")
    
if not APIFY_TOKEN:
    logger.warning("APIFY_API_TOKEN not found in .env file. Some features may not work.")
else:
    client = ApifyClient(APIFY_TOKEN)               # ApifyClient init

def get_page_id(name_or_slug: str) -> str | None:
    """
    Attempts to resolve a Facebook Page ID using multiple methods:
      1. Direct node lookup by slug
      2. Search by name with exact match
      3. Search by name with fuzzy match
    
    Returns the numeric page ID or None if not found.
    """
    if not GRAPH_API_TOKEN:
        logger.error("Cannot get page ID: FB_GRAPH_TOKEN is missing")
        return None
        
    # Clean up the name for better matching
    clean_name = name_or_slug.strip()
    logger.info(f"Attempting to find page ID for: {clean_name}")
    
    # 1. Try direct slug lookup (for exact matches/official pages)
    try:
        resp = requests.get(
            f"https://graph.facebook.com/v17.0/{clean_name}",
            params={"fields": "id,name", "access_token": GRAPH_API_TOKEN},
        )
        if resp.status_code == 200:
            data = resp.json()
            page_id = data.get("id")
            logger.info(f"Direct lookup successful: {data.get('name')} (ID: {page_id})")
            return page_id
    except Exception as e:
        logger.warning(f"Direct page lookup failed: {str(e)}")
    
    # 2. Try search endpoint with exact match preference
    try:
        resp = requests.get(
            "https://graph.facebook.com/v17.0/search",
            params={
                "type": "page",
                "q": clean_name,
                "fields": "id,name,verification_status,fan_count",
                "limit": 5,  # Get multiple results to find best match
                "access_token": GRAPH_API_TOKEN,
            },
        )
        
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            
            if data:
                # Try to find verified pages first
                verified_pages = [p for p in data if p.get("verification_status") == "blue_verified"]
                if verified_pages:
                    page_id = verified_pages[0]["id"]
                    logger.info(f"Found verified page: {verified_pages[0].get('name')} (ID: {page_id})")
                    return page_id
                
                # Then try pages with most fans
                if len(data) > 1:
                    # Sort by fan count if available
                    data.sort(key=lambda x: x.get("fan_count", 0), reverse=True)
                
                page_id = data[0]["id"]
                logger.info(f"Search found: {data[0].get('name')} (ID: {page_id})")
                return page_id
            else:
                logger.warning(f"No pages found for query: {clean_name}")
    except Exception as e:
        logger.warning(f"Page search failed: {str(e)}")

    # 3. If all else fails, try with delay and modified query
    try:
        time.sleep(1)  # Avoid rate limiting
        modified_query = clean_name.split('&')[0].strip()  # Try with first part of name only
        
        if modified_query != clean_name:
            logger.info(f"Trying modified query: {modified_query}")
            resp = requests.get(
                "https://graph.facebook.com/v17.0/search",
                params={
                    "type": "page",
                    "q": modified_query,
                    "fields": "id,name",
                    "access_token": GRAPH_API_TOKEN,
                },
            )
            
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    page_id = data[0]["id"]
                    logger.info(f"Found with modified query: {data[0].get('name')} (ID: {page_id})")
                    return page_id
    except Exception as e:
        logger.warning(f"Modified search failed: {str(e)}")
    
    logger.error(f"Could not find page ID for: {name_or_slug}")
    return None

def build_ads_library_url(page_id_or_name: str) -> str:
    """
    Constructs the Facebook Ads Library URL for ads.
    If page_id is provided, uses page search method.
    If page_id is None, falls back to keyword search.
    """
    base = "https://www.facebook.com/ads/library/"
    
    # If we have a valid page ID, use page search
    if page_id_or_name and page_id_or_name.isdigit():
        params = {
            "active_status": "active",
            "ad_type": "all",
            "country": "ALL",
            "is_targeted_country": "false",
            "media_type": "video",    # only video ads
            "search_type": "page",
            "view_all_page_id": page_id_or_name,
        }
    else:
        # Fall back to keyword search if no valid page ID
        logger.info(f"Using keyword search for: {page_id_or_name}")
        params = {
            "active_status": "active",
            "ad_type": "all",
            "country": "ALL",
            "is_targeted_country": "false",
            "media_type": "video",    # only video ads
            "search_type": "keyword_unordered",
            "q": page_id_or_name,
        }
    
    return f"{base}?{urlencode(params)}"

def scrape_competitor_ads_by_name(
    competitor_names: list[str],
    max_items: int = 100,
) -> list[dict]:
    """
    1. Converts each name → page ID
    2. Builds Ads Library URL
    3. Runs the Apify actor and returns all video ads
    """
    # 1 & 2. Build URL list for actor input
    ads_urls = []
    for name in competitor_names:
        page_id = get_page_id(name)
        logger.info(f"For competitor '{name}', found page_id: {page_id}")
        
        # Use the page ID if found, otherwise fall back to the name
        search_term = page_id if page_id else name
        url = build_ads_library_url(search_term)
        
        ads_urls.append({"url": url})
    
    if not APIFY_TOKEN:
        logger.error("Cannot run Apify actor: APIFY_API_TOKEN is missing")
        return ads_urls
    
    # 3. Call the Ads Library actor
    run_input = {
        "urls": ads_urls,
        "count": max_items,
        "scrapePageAds.activeStatus": "active",
        "period": "",        # all time
    }
    
    try:
        logger.info(f"Starting Apify actor run with {len(ads_urls)} URLs")
        run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
        
        # 4. Collect all ad records
        dataset_id = run.get("defaultDatasetId")
        if dataset_id:
            results = list(client.dataset(dataset_id).iterate_items())
            logger.info(f"Successfully scraped {len(results)} ad records")
            return results
        else:
            logger.error("Failed to get dataset ID from Apify run")
            return []
    except Exception as e:
        logger.error(f"Error running Apify actor: {str(e)}")
        # Return the URLs we would have scraped
        return ads_urls

if __name__ == "__main__":
    # Replace with your actual competitor names
    competitors = ['Levi Strauss & Co.', 'Wrangler', 'Lee'] # get_competitors(q, top_k=20)
    logger.info(f"Processing competitors: {competitors}")
    
    # Scrape ads
    ads = scrape_competitor_ads_by_name(competitors, max_items=15)
    
    # Save to JSON file
    output_path = "meta_profile_ads.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ads, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Saved {len(ads)} ad records to '{output_path}'")
