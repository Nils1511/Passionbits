# from apify_client import ApifyClient
# import json

# # Initialize the ApifyClient with your API token
# client = ApifyClient(os.getenv('APIFY_API_TOKEN'))  # ← replace with your real token

# # Prepare the Actor input to scrape ads/results for “polo shirts”
# run_input = {
#     "query": "polo shirts",          # change search term
#     "maxPages": 500,                   # pages of results to fetch
#     "proxyConfiguration": {
#         "useApifyProxy": True,        # keep using Apify Proxy
#         "apifyProxyCountry": "US"
#     },
# }

# # Run the same Actor (ID: 60AtqWgevwexQsFPw) and wait for it to finish
# run = client.actor("60AtqWgevwexQsFPw").call(run_input=run_input)

# # Collect all items from the run’s default dataset
# results = list(client.dataset(run["defaultDatasetId"]).iterate_items())

# # Save the results into a JSON file
# with open("tiktok_polo_shirts_ads.json", "w", encoding="utf-8") as f:
#     json.dump(results, f, ensure_ascii=False, indent=4)

# print("Results saved to tiktok_polo_shirts_ads.json")


from apify_client import ApifyClient
import json
import os

# 1. Initialize client
API_TOKEN = os.getenv('APIFY_API_TOKEN')  # os.getenv("APIFY_TOKEN", "<YOUR_API_TOKEN>")
client = ApifyClient(API_TOKEN)

# 2. Prepare Actor input for #poloshirts, up to 500 items
run_input = {
    "hashtags": ["poloshirts"],        # omit the leading “#”
    "resultsPerPage": 100,             # pages of 100 items each
    "maxItems": 500,                   # stop after 500 total
    "shouldDownloadVideos": False,
    "shouldDownloadCovers": False,
    "shouldDownloadSubtitles": False,
    "shouldDownloadSlideshowImages": False,
}

# 3. Run the TikTok Hashtag Scraper actor
run = client.actor("f1ZeP0K58iwlqG2pY").call(run_input=run_input)

# 4. Fetch all items and save to JSON
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
output_file = "tiktok_poloshirts_500.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)

print(f"✅ Retrieved {len(items)} TikToks for #poloshirts and saved to {output_file}")
