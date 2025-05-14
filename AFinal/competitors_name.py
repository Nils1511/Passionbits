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
) -> list[str]:
    """
    Query Gemini for the top competing brands of a given product/brand,
    optionally filtered by a price range.

    Args:
      query: The product or brand to analyze (e.g. "Snitch's Cotton Shirts").
      top_k: Number of competitors to return.
      price_min: Minimum price filter (inclusive).
      price_max: Maximum price filter (inclusive).

    Returns:
      A list of competitor brand/seller names.
    """
    # Build the base prompt
    contents = (
        f"List the full name of top {top_k} competing brands or sellers for “{query}”."
    )
    # Inject price‑range requirement if both bounds are given
    # if price_min is not None and price_max is not None:
    #     contents += (
    #         f" Only include competitors whose prices range from "
    #         f"${price_min:.2f} to ${price_max:.2f}."
    #     )
    # Ask for comma‑separated names only
    contents += " Provide only the brand/seller names, separated by commas."

    # Call the Gemini model
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=contents,
    )  # :contentReference[oaicite:2]{index=2}

    # Parse out the comma‑separated list
    text = response.text or ""
    competitors = [name.strip() for name in text.split(",") if name.strip()]
    return competitors

# if __name__ == "__main__":
#     q = input("Enter product or brand query: ").strip()
#     # Example with price filter from $20 to $50
#     competitors = get_competitors(q, top_k=20)   #, price_min=20.0, price_max=50.0)
#     print(f"Top competitors for '{q}' in :")
#     for i, c in enumerate(competitors, 1):
#         print(f"{i}. {c}")
