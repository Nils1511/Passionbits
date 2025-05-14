import json
import os
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('APIFY_API_KEY')
client = ApifyClient(API_TOKEN)

# List of Instagram handles to scrape
usernames = [
    "hm",
    "uniqloin",
    "snitch.co.in",
    "tommyhilfiger",
]

# How many reels to fetch per profile
results_limit = 100

# Actor ID for the Instagram reels scraper
ACTOR_ID = "xMc5Ga1oCONPmWJIa"

for username in usernames:
    print(f" Fetching {results_limit} reels for @{username}...")
    # Prepare input for this run
    run_input = {
        "username": [username],
        "resultsLimit": results_limit,
    }

    # Kick off the actor and wait for it to finish
    run = client.actor(ACTOR_ID).call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]

    # Iterate through all items in the dataset
    reels = []
    for item in client.dataset(dataset_id).iterate_items():
        reels.append(item)

    # Save to a JSON file named after the profile
    filename = f"{username}_reels.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(reels, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(reels)} reels to {filename}\n")
