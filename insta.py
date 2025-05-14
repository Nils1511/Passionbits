from apify_client import ApifyClient
import json
import os
# Initialize the ApifyClient with your API token
client = ApifyClient(os.getenv('APIFY_API_TOKEN'))

# Prepare the Actor input to scrape #poloshirts posts
run_input = {
#     "search": "poloshirts",            # Hashtag to search
#     "searchType": "hashtag",           # Search type
#     "resultsType": "stories",            # Type of results to fetch
#     "resultsLimit": 10,               # Limit of posts to fetch
#     "searchLimit": 1,                  # Limit of hashtag search results
#     "addParentData": False,            # Exclude parent hashtag metadata
# }
 
  "search": "poloshirts",
  "searchType": "hashtag",
  "resultsType": "stories",
  "searchLimit": 2,            # scan more pages
  "resultsLimit": 50,         # fetch up to 1,000 posts per page
  "useSessionPool": True,       # maintain sessions
  "proxyConfiguration": {
    "useApifyProxy": True,
    "apifyProxyGroups": ["RESIDENTIAL"]
  },
  "requestTimeoutSecs": 60,
  "pageLoadTimeoutSecs": 60,
  "maxRequestRetries": 5
}


# Run the Actor and wait for it to finish
run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

# Fetch all items from the dataset
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
filtered = [
    item for item in items
        # if item.get("likesCount", 0) > 100                   # ensure likes exceed threshold :contentReference[oaicite:7]{index=7}
]
# Save the items to a local JSON file
with open("instagram_polo_shirts_reels.json", "w", encoding="utf-8") as f:
    json.dump(filtered, f, ensure_ascii=False, indent=2)

print('Saved')


