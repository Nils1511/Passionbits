# from apify_client import ApifyClient
# import json

# # Initialize the ApifyClient with your API token
# client = ApifyClient(os.getenv('APIFY_API_TOKEN'))  # replace with your token

# # Prepare the Actor input to search for 'polo shirts'
# run_input = {
#     "searchQueries": ["polo shirts"],   # search term to scrape :contentReference[oaicite:0]{index=0}
#     "maxResults": 200,                     # limit to 2 video results :contentReference[oaicite:1]{index=1}
#     "maxResultsShorts": 100,               # skip Shorts results :contentReference[oaicite:2]{index=2}
#     "maxResultStreams": 0,               # skip live streams :contentReference[oaicite:3]{index=3}
#     "proxyConfiguration": {
#         "useApifyProxy": True,
#         "apifyProxyGroups": ["RESIDENTIAL"],
#         "apifyProxyCountry": "IN",
#     },
# }

# # Run the YouTube Scraper Actor and wait for it to finish
# run = client.actor("streamers/youtube-scraper").call(run_input=run_input)  # Actor “streamers/youtube‑scraper” :contentReference[oaicite:4]{index=4}

# # Fetch and collect all items from the default dataset
# results = []
# for item in client.dataset(run["defaultDatasetId"]).iterate_items():
#     results.append(item)

# # Save the scraped data to a JSON file
# with open("300_youtube_polo_shirts_ads.json", "w", encoding="utf-8") as f:
#     json.dump(results, f, ensure_ascii=False, indent=4)

# print("Results saved to youtube_polo_shirts_ads.json")

from apify_client import ApifyClient
import json
import os

# --- 1. Initialize the ApifyClient with your API token ---
# Replace "<YOUR_API_TOKEN>" with your actual Apify API token (keep it secret!)
API_TOKEN = os.getenv('APIFY_API_TOKEN')  #os.getenv("APIFY_TOKEN", "<YOUR_API_TOKEN>")
client = ApifyClient(API_TOKEN)

# --- 2. Prepare the actor input for scraping Shorts only ---
run_input = {
    "hashtags": ["poloshirts"],      # the hashtag(s) to search for (without the # sign)
    "maxResults": 500,                 # we’re not interested in full-length videos
    "scrapeShortsOnly": True,        # enforce that only Shorts are scraped
            
}

# --- 3. Run the actor and wait for it to finish ---
# Actor ID: 89uTe0zmDUIatNKSd (YouTube Hashtag Scraper)
run = client.actor("89uTe0zmDUIatNKSd").call(run_input=run_input)

# --- 4. Fetch all items from the default dataset ---
results = []
dataset = client.dataset(run["defaultDatasetId"])
for item in dataset.iterate_items():
    # The scraper already filtered to Shorts only,
    # but you can add further filtering here if needed.
    results.append(item)

# --- 5. Save the results to a JSON file ---
output_filename = "youtube_500_shorts_poloshirts.json"
with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print(f"✅ Saved {len(results)} Shorts for #poloshirts to {output_filename}")
