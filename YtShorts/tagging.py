import os
import time
import json
import ast
from google import genai
from google.genai import types
from google.genai.errors import ClientError
import torch
from dotenv import load_dotenv

load_dotenv() 

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY     = os.getenv('GEMINI_API_KEY', 'YOUR_API_KEY_HERE')
MODEL_NAME  = "gemini-2.0-flash-001"
INPUT_FILE  = "yt_shorts_filtered.json"    # JSON array of video entries
OUTPUT_DIR  = "tagged_yt_shorts"
DEVICE       = "cuda" if torch.cuda.is_available() else "cpu"
MAX_RETRIES = 5
RETRY_DELAY  = 5

# ─── TAG CATEGORIES ────────────────────────────────────────────────────────────
HIERARCHY_TAGS = ["product", "category", "industry", "brand", "none"]
STORYLINE_TAGS = [
    "unboxing", "testimonial", "before-after", "tutorial", "listicle",
    "daily-routine", "voice-over-showcase", "dialogue", "replicate-ad",
    "demonstration", "none"
]
HOOK_TAGS = [
    "strong-reaction", "dramatize-problem", "absurd-alternative",
    "visual-trick", "highlight-popularity", "target-audience-callout",
    "controversy", "emphasize-one-usp", "none"
]
CTA_TAGS = [
    "buy_now", "download_now", "visit_website", "sign_up", "subscribe",
    "start_free_trial", "learn_more", "none"
]
ICP_TAGS = ["moms", "athletes", "students", "travelers", "golfers"]
ACTOR_TAGS = ["male", "female", "mixed", "none"]
# ────────────────────────────────────────────────────────────────────────────────

def prepare_prompt(video_url: str, title: str) -> str:
    base = (
        f"Video URL: {video_url}\n"
        f"Title: {title}\n"
        "You are a content-tagging assistant. Assign exactly one tag for each of:\n"
        "  • hierarchy_tag\n"
        "  • storyline_tag\n"
        "  • hook_tag\n"
        "  • cta_tag\n"
        "  • actor_tag\n"
        "Then choose exactly one icp_tag (ideal customer persona) from the provided list.\n\n"
        f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
        f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
        f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
        f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
        f"ACTOR_TAGS: {', '.join(ACTOR_TAGS)}\n"
        f"ICP_TAGS: {', '.join(ICP_TAGS)}\n\n"
        "ICP Definitions (choose exactly one):\n"
        "  • moms       : the video features a woman with a child or baby bump, parenting tips, nursery scenes, family routines,\n"
        "                  baby products, or mom-focused voice-over.\n"
        "  • athletes   : the video contains athletic activity (running, gym workouts, sports gear), sporty clothing, coaches/trainers,\n"
        "                  fitness metrics, competitive or performance imagery.\n"
        "  • students   : scenes of classrooms, textbooks, studying setups, backpacks, campus life, teachers explaining concepts,\n"
        "                  exam prep, or youth-oriented slang.\n"
        "  • travelers  : travel footage (landmarks, suitcases, boarding passes), exotic locations, hotel/hostel scenes,\n"
        "                  flight or train shots, itineraries, or voice-over about exploring.\n"
        "  • golfers    : golf courses, clubs/putters, tee shots, fairways/greens, golf attire (polo shirts, visors),\n"
        "                  swing tutorials, caddie interactions, or scoring overlays.\n\n"
        "Return only a JSON with keys: hierarchy_tag, storyline_tag, hook_tag, cta_tag, actor_tag, icp_tag."
    )
    return base

def generate_tags(client, prompt: str) -> dict:
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=(
                        "You are a content-tagging assistant. Given video metadata, choose the best single tag from each provided list."
                    ),
                    temperature=0.0,
                    max_output_tokens=150,
                ),
            )
            raw = resp.text.strip()
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                start, end = raw.find("{"), raw.rfind("}")
                if start != -1 and end != -1:
                    return json.loads(raw[start:end+1])
                return {}
        except ClientError:
            attempts += 1
            time.sleep(RETRY_DELAY)
    # fallback to 'none'
    return {key: 'none' for key in ['hierarchy_tag','storyline_tag','hook_tag','cta_tag','actor_tag','icp_tag']}


def main():
    client = genai.Client(api_key=API_KEY)

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        entries = json.load(f)   # → entries is now a list of dicts

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tagged_results = []

    for entry in entries:
        video_url = entry.get('url', '')
        title     = entry.get('title', '')

        prompt = prepare_prompt(video_url, title)
        tags   = generate_tags(client, prompt)

        combined = {**entry, **tags}
        tagged_results.append(combined)
        time.sleep(0.1)


    # Save all
    all_path = os.path.join(OUTPUT_DIR, 'all_tagged_shorts.json')
    with open(all_path, 'w', encoding='utf-8') as f:
        json.dump(tagged_results, f, ensure_ascii=False, indent=2)
    print(f"Saved all tagged entries to {all_path}")

    # Save by persona
    for persona in ICP_TAGS:
        subset = [e for e in tagged_results if e.get('icp_tag', '').lower() == persona]
        if subset:
            path = os.path.join(OUTPUT_DIR, f"tagged_{persona}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(subset)} entries for persona '{persona}' to {path}")

if __name__ == "__main__":
    main()