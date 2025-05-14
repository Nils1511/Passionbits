import json
import os
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv() 
# Initialize the ApifyClient with your API token
API_TOKEN = os.getenv('APIFY_API_KEY')
client = ApifyClient(API_TOKEN)

def fetch_ads():
    # Prepare the Actor input for PoloShirts ads
    run_input = {
        "urls": [
            {
                "url": (
                    "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=video&q=Polo%20Shirts&search_type=keyword_unordered"
                )
            }
        ],
        "count": 1000,
        "scrapePageAds.activeStatus": "all",
        "period": ""
    }

    # Run the Actor
    print("Starting Apify Actor run for PoloShirts ads...")
    run = client.actor("XtaWFhbtfxyzqrFmd").call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]
    print(f"Actor run started (ID: {run['id']}), dataset ID: {dataset_id}")

    # Iterate over all items in the dataset
    print("Fetching items from dataset...")
    all_items = []
    for item in client.dataset(dataset_id).iterate_items():
        all_items.append(item)

    print(f"Fetched {len(all_items)} items.")
    return all_items

def save_to_json(data, filename="poloshirts_meta_ads.json"):
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
