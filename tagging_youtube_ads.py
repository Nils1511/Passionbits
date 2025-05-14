# #!/usr/bin/env python3
# import os
# import json
# import time
# import subprocess
# from pytube import YouTube
# from PIL import Image
# import pandas as pd
# import torch
# from transformers import CLIPProcessor, CLIPModel
# from google import genai
# from google.genai import types

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# API_KEY      = os.getenv('GEMINI_API_KEY')       # ← replace with your Gemini API key
# MODEL_NAME   = "gemini-2.0-flash-001"
# INPUT_FILE   = "/content/youtube_filtered_polo_shirts_ads.csv"  # CSV with columns: title, url
# OUTPUT_FILE  = "youtube_tagged_output.json"
# DEVICE       = "cuda" if torch.cuda.is_available() else "cpu"
# # ────────────────────────────────────────────────────────────────────────────────

# # Initialize CLIP
# clip_processor = CLIPProcessor.from_pretrained(
#     "laion/CLIP-ViT-B-32-laion2B-s34B-b79K"
# )
# clip_model = CLIPModel.from_pretrained(
#     "laion/CLIP-ViT-B-32-laion2B-s34B-b79K"
# ).to(DEVICE)

# # Tag categories
# HIERARCHY_TAGS = ["product","category","industry","brand","none"]
# STORYLINE_TAGS = [
#     "unboxing","testimonial","before-after","tutorial","listicle",
#     "daily-routine","voice-over-showcase","dialogue","replicate-ad",
#     "demonstration","none"
# ]
# HOOK_TAGS = [
#     "strong-reaction","dramatize-problem","absurd-alternative",
#     "visual-trick","highlight-popularity","target-audience-callout",
#     "controversy","emphasize-one-usp","none"
# ]
# CTA_TAGS = [
#     "buy_now","download_now","visit_website","sign_up","subscribe",
#     "start_free_trial","learn_more","none"
# ]
# ICP_TAGS = ["moms","athletes","students","golfers","travelers","none"]
# ACTOR_TAGS = ["male","female","mixed","none"]

# CLIP_ACTOR_MAP = {
#     "a male actor speaking":   "male",
#     "a female actor speaking": "female",
#     "multiple people in frame": "mixed",
#     "no people visible":       "none",
# }

# # ─── HELPERS ────────────────────────────────────────────────────────────────────
# def download_youtube(url: str, out: str, timeout: int = 60) -> str:
#     """Download YouTube video via pytube."""
#     try:
#         yt = YouTube(url)
#         stream = yt.streams.filter(progressive=True, file_extension="mp4").order_by('resolution').desc().first()
#         stream.download(filename=out)
#         return out
#     except Exception as e:
#         print(f"[Warning] YouTube download failed: {e}")
#         return None


# def extract_frames(video_path: str, times=(0,4,8,12,16,20,24,28)) -> list:
#     """Extract one frame at each timestamp via ffmpeg."""
#     frames = []
#     for i, t in enumerate(times):
#         out = f"frame_{i}.jpg"
#         try:
#             subprocess.run([
#                 "ffmpeg","-ss", str(t), "-i", video_path,
#                 "-frames:v","1","-q:v","2", out, "-y"
#             ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
#             if os.path.exists(out):
#                 frames.append(out)
#         except subprocess.CalledProcessError:
#             continue
#     return frames


# def clip_classify(frames: list, mapping: dict) -> str:
#     """Run CLIP over frames and pick the highest‑scoring label."""
#     if not frames:
#         return "none"
#     imgs = []
#     for f in frames:
#         try:
#             imgs.append(Image.open(f).convert("RGB"))
#         except IOError:
#             continue
#     if not imgs:
#         return "none"
#     texts = list(mapping.keys())
#     inputs = clip_processor(text=texts, images=imgs, padding=True, return_tensors="pt").to(DEVICE)
#     with torch.no_grad():
#         logits = clip_model(**inputs).logits_per_image
#         probs = logits.softmax(dim=-1).mean(dim=0).tolist()
#     best = probs.index(max(probs))
#     return mapping[texts[best]]


# # def prepare_prompt(url: str, title: str) -> str:
# #     """Build the prompt for Gemini tagging with only URL and Title."""
# #     return (
# #         f"Video URL: {url}\n"
# #         f"Title: {title}\n\n"
# #         "Assign one tag per category. Return only a JSON object with keys: "
# #         "hierarchy_tag, storyline_tag, hook_tag, cta_tag, icp_tag, actor_tag.\n\n"
# #         f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
# #         f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
# #         f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
# #         f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
# #         f"ICP_TAGS: {', '.join(ICP_TAGS)}\n"
# #         f"ACTOR_TAGS: {', '.join(ACTOR_TAGS)}\n"
# #     )

# def prepare_prompt(url: str, title: str) -> str:
#     """Build the prompt for Gemini tagging, with special ICP rules."""
#     return (
#         f"Video URL: {url}\n"
#         f"Title: {title}\n\n"
#         "You are a content‑tagging assistant. Assign exactly one tag for each of:\n"
#         "  • hierarchy_tag\n"
#         "  • storyline_tag\n"
#         "  • hook_tag\n"
#         "  • cta_tag\n"
#         "For ICP_TAGS, assign **at least one** and **at most two** tags:\n"
#         "  - If the ad clearly speaks to two audience segments, return both (comma‑separated).\n"
#         "  - Only use “none” if absolutely no listed segment applies.\n\n"
#         "Return **only** a JSON object with these keys:\n"
#         "  { hierarchy_tag, storyline_tag, hook_tag, cta_tag, icp_tag, actor_tag }\n\n"
#         f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
#         f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
#         f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
#         f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
#         f"ICP_TAGS (choose 1–2, minimize “none”): {', '.join(ICP_TAGS)}\n"
#     )

# def generate_tags(client, prompt: str) -> dict:
#     """Call Gemini model to generate tag JSON for a single prompt."""
#     resp = client.models.generate_content(
#         model=MODEL_NAME,
#         contents=prompt,
#         config=types.GenerateContentConfig(
#             system_instruction=(
#                 "You are a content-tagging assistant. Given video metadata, "
#                 "choose the best single tag from each provided list."
#             ),
#             temperature=0.0,
#             max_output_tokens=100,
#         ),
#     )
#     raw = resp.text.strip()
#     try:
#         return json.loads(raw)
#     except Exception:
#         # Fallback to extract JSON substring
#         start, end = raw.find("{"), raw.rfind("}")
#         if start != -1 and end != -1:
#             return json.loads(raw[start:end+1])
#         return {k: 'none' for k in ['hierarchy_tag','storyline_tag','hook_tag','cta_tag','icp_tag','actor_tag']}


# def main():
#     # Initialize clients
#     client = genai.Client(api_key=API_KEY)
#     df = pd.read_csv(INPUT_FILE)
#     results = []

#     for idx, row in df.iterrows():
#         title, url = row['title'], row['url']
#         # Gemini tags
#         prompt = prepare_prompt(url, title)
#         tags = generate_tags(client, prompt)

#         # CLIP-based actor_tag
#         actor = 'none'
#         vid_path = f"video_{idx}.mp4"
#         vid = download_youtube(url, out=vid_path)
#         if vid:
#             frames = extract_frames(vid)
#             actor = clip_classify(frames, CLIP_ACTOR_MAP)
#             # cleanup
#             for f in frames:
#                 os.remove(f)
#             os.remove(vid)

#         entry = {**row.to_dict(), **tags, 'actor_tag': actor}
#         results.append(entry)

#     with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
#         json.dump(results, out_f, ensure_ascii=False, indent=2)

#     print(f"Tagged {len(results)} records and saved to {OUTPUT_FILE}")

# if __name__ == "__main__":
#     main()


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
API_KEY     = os.getenv('GEMINI_API_KEY')
MODEL_NAME  = "gemini-2.0-flash-001"
INPUT_FILE  = "yt_filtered.json"    # JSON array of video entries
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
    """Build the prompt for tagging this YouTube Short."""
    return (
        f"Video URL: {video_url}\n"
        f"Title: {title}\n\n"
        "You are a content-tagging assistant. Assign exactly one tag for each of:\n"
        "  • hierarchy_tag\n"
        "  • storyline_tag\n"
        "  • hook_tag\n"
        "  • cta_tag\n"
        "  • actor_tag\n"
        "Choose exactly one ideal target persona (icp_tag) from the provided list based on the video content."
        " If none of the listed personas fit, choose the one that best aligns with the content.\n"
        "Return only a JSON object with keys: { hierarchy_tag, storyline_tag, hook_tag, cta_tag, actor_tag, icp_tag }\n\n"
        f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
        f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
        f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
        f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
        f"ACTOR_TAGS: {', '.join(ACTOR_TAGS)}\n"
        f"ICP_TAGS (choose exactly one, pick best if none fits): {', '.join(ICP_TAGS)}\n"
    )


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
