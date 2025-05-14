from apify_client import ApifyClient
import json
import os

# --- 1. Initialize the ApifyClient with your API token ---
# Replace "<YOUR_API_TOKEN>" with your actual Apify API token (keep it secret!)
API_TOKEN = os.getenv('APIFY_API_KEY')
client = ApifyClient(API_TOKEN)

def fetch_ads():
    run_input = {
        "hashtags": ["poloshirts"],      # the hashtag(s) to search for (without the # sign)
        "maxResults": 500,                 # we’re not interested in full-length videos
        "scrapeShortsOnly": True,        # enforce that only Shorts are scraped
                
    }


    # Run the Actor
    print("Starting Apify Actor run for PoloShirts ads...")
    run = client.actor("89uTe0zmDUIatNKSd").call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]
    print(f"Actor run started (ID: {run['id']}), dataset ID: {dataset_id}")

    # Iterate over all items in the dataset
    print("Fetching items from dataset...")
    all_items = []
    for item in client.dataset(dataset_id).iterate_items():
        all_items.append(item)

    print(f"Fetched {len(all_items)} items.")
    return all_items

def save_to_json(data, filename="yt_shorts_poloshirts.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved data to {filename}")


def main():
    """Main function to orchestrate the scraping workflow."""
    try:
        ads = fetch_ads()
        save_to_json(ads)
        return True
    except Exception as e:
        print(f"Error in scraping.py: {str(e)}")
        return False

if __name__ == "__main__":
    main()
