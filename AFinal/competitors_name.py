import os
from google import genai  # Google Gen AI SDK import :contentReference[oaicite:0]{index=0}
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('GEMINI_API_KEY')
# Initialize the client with your free‑tier key
client = genai.Client(api_key=API_TOKEN)  # no Vertex AI needed :contentReference[oaicite:1]{index=1}

def get_competitors(
    query: str,
    top_k: int = 5,
    # price_min: float | None = None,
    # price_max: float | None = None,
) -> list[tuple[str, str, str]]:
    """
    Query Gemini for the top competing brands of a given product/brand,
    returning (brand, facebook_url, instagram_username) tuples.
    """
    contents = (
        f"List the top {top_k} competing brands or sellers for \"{query}\". "
        "For each, provide strictly:\n"
        "1. The full, official brand or seller name (no abbreviations)\n"
        "2. The official Facebook page URL for the brand or seller (must be a real, public Facebook page, not a search or unofficial page).\n"
        "3. The official Instagram username for the brand or seller (do not include @, just the username; if not available, write 'N/A').\n"
        "If a Facebook page or Instagram username is not available, write 'N/A' for that field.\n"
        "Output a single comma-separated list, where each item is in this exact format: Brand Name | Facebook URL | Instagram Username\n"
        "Do not include any commentary, explanation, or extra text. Only output the list."
    )

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=contents,
    )

    text = response.text or ""
    competitors = []
    for line in text.split(","):
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 3:
            competitors.append((parts[0], parts[1], parts[2]))
        elif len(parts) == 2:
            competitors.append((parts[0], parts[1], "N/A"))
        elif len(parts) == 1:
            competitors.append((parts[0], "N/A", "N/A"))
    return competitors

# if __name__ == "__main__":
#     q = input("Enter product or brand query: ").strip()
#     # Example with price filter from $20 to $50
#     competitors = get_competitors(q, top_k=10)   #, price_min=20.0, price_max=50.0)
#     print(competitors)
#     print(f"Top competitors for '{q}' in :")
#     for i, c in enumerate(competitors, 1):
#         print(f"{i}. {c}")
