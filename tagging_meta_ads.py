# #!/usr/bin/env python3
# import os
# import csv
# import time
# from PIL import Image
# import pandas as pd
# import json
# from google import genai
# from google.genai import types
# from transformers import CLIPProcessor, CLIPModel
# import torch
# import requests
# import subprocess

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# API_KEY      = os.getenv('GEMINI_API_KEY')
# MODEL_NAME   = "gemini-2.0-flash-001"
# INPUT_FILE   = "/content/filtered_Polo_shirts_ads.csv"  # CSV with video_hd_url, title, text
# OUTPUT_FILE  = "tagged_output.json"
# DEVICE        = "cuda" if torch.cuda.is_available() else "cpu"
# # ────────────────────────────────────────────────────────────────────────────────

# clip_processor = CLIPProcessor.from_pretrained(
#     "laion/CLIP-ViT-B-32-laion2B-s34B-b79K"
# )
# clip_model = CLIPModel.from_pretrained(
#     "laion/CLIP-ViT-B-32-laion2B-s34B-b79K"
# ).to(DEVICE)

# # ─── TAG CATEGORIES ────────────────────────────────────────────────────────────
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
# ICP_TAGS = ["moms","athletes","students","none"]
# ACTOR_TAGS = ["male","female","mixed","none"]
# # ────────────────────────────────────────────────────────────────────────────────

# CLIP_ACTOR_MAP = {
#     "a male actor speaking":   "male",
#     "a female actor speaking": "female",
#     "multiple people in frame": "mixed",
#     "no people visible":       "none",
# }


# def download_video(url, out="input.mp4", timeout=10):
#     """Download remote video via HTTP streaming, with error handling."""
#     try:
#         r = requests.get(url, stream=True, timeout=timeout)
#         r.raise_for_status()
#         with open(out, "wb") as f:
#             for chunk in r.iter_content(1 << 20):
#                 f.write(chunk)
#         return out
#     except requests.RequestException as e:
#         print(f"[Warning] Video download failed: {e}")
#         return None


# def extract_frames(video_path, times=(0,4,8,12,16,20,24,28)):
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


# def clip_classify(frames, mapping):
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


# def prepare_prompt(url: str, title: str, text: str) -> str:
#     """Build the prompt for Gemini tagging."""
#     return (
#         f"Video URL: {url}\n"
#         f"Title: {title}\n"
#         f"Text: {text}\n\n"
#         "Assign one tag per category. Return only a JSON object with keys: "
#         "hierarchy_tag, storyline_tag, hook_tag, cta_tag, icp_tag, actor_tag.\n\n"
#         f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
#         f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
#         f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
#         f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
#         f"ICP_TAGS: {', '.join(ICP_TAGS)}\n"
#         f"ACTOR_TAGS: {', '.join(ACTOR_TAGS)}\n"
#     )


# def generate_tags(client, prompt: str) -> dict:
#     """Call Gemini model to generate tag JSON for a single prompt."""
#     resp = client.models.generate_content(
#         model=MODEL_NAME,
#         contents=prompt,
#         config=types.GenerateContentConfig(
#             system_instruction=(
#                 "You are a content-tagging assistant. "
#                 "Given video metadata, choose the best single tag from each provided list."
#             ),
#             temperature=0.0,
#             max_output_tokens=100,
#         ),
#     )
#     raw = resp.text.strip()
#     try:
#         return json.loads(raw)
#     except Exception:
#         start, end = raw.find("{"), raw.rfind("}")
#         if start != -1 and end != -1:
#             return json.loads(raw[start:end+1])
#         return {k: 'none' for k in ['hierarchy_tag','storyline_tag','hook_tag','cta_tag','icp_tag','actor_tag']}


# def main():
#     client = genai.Client(api_key=API_KEY)
#     df = pd.read_csv(INPUT_FILE)
#     results = []

#     for idx, row in df.iterrows():
#         url, title, text = row['video_hd_url'], row['title'], row['text']
#         # Gemini tags
#         prompt = prepare_prompt(url, title, text)
#         tags = generate_tags(client, prompt)

#         # CLIP-based actor_tag with error resilience
#         actor = 'none'
#         vid = download_video(url, out=f"video_{idx}.mp4")
#         if vid:
#             frames = extract_frames(vid)
#             actor = clip_classify(frames, CLIP_ACTOR_MAP)
#             # clean up
#             for f in frames:
#                 os.remove(f)
#             os.remove(vid)

#         entry = {**row.to_dict(), **tags, 'actor_tag': actor}
#         results.append(entry)

#     with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
#         json.dump(results, f, ensure_ascii=False, indent=2)

#     print(f"Tagged {len(results)} records and saved to {OUTPUT_FILE}")

# if __name__ == "__main__":
#     main()


# #!/usr/bin/env python3
# import os
# import time
# import json
# import pandas as pd
# from google import genai
# from google.genai import types
# import torch

# # ─── CONFIG ────────────────────────────────────────────────────────────────────
# API_KEY      = os.getenv('GEMINI_API_KEY')
# MODEL_NAME   = "gemini-2.0-flash-001"
# INPUT_FILE   = "290_filtered_meta_video_Polo_shirts_ads.csv"
# OUTPUT_FILE  = "290_tagged_meta_ads_polo_shirts.json"
# DEVICE        = "cuda" if torch.cuda.is_available() else "cpu"
# # ────────────────────────────────────────────────────────────────────────────────

# # ─── TAG CATEGORIES ────────────────────────────────────────────────────────────
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
# ICP_TAGS = ["moms","athletes","students","travelers","golfers","young professionals","none"]
# ACTOR_TAGS = ["male","female","mixed","none"]
# # ────────────────────────────────────────────────────────────────────────────────

# ACTOR_MAP = {
#     "a male actor speaking":   "male",
#     "a female actor speaking": "female",
#     "multiple people in frame": "mixed",
#     "no people visible":       "none",
# }


# def prepare_prompt(url: str, title: str, text: str, heirarchy: str, description: str, cta: str) -> str:
#     """Build the prompt for Gemini tagging."""
#     return (
#         f"Video URL: {url}\n"
#         f"Title: {title}\n"
#         f"Text: {text}\n"
#         f"Heirarchy: {heirarchy}\n"
#         f"CTA Type: {cta}\n"
#         f"Link description: {description}\n\n"
#         "You are a content-tagging assistant. Assign exactly one tag for each of:\n"
#         "  • hierarchy_tag\n"
#         "  • storyline_tag\n"
#         "  • hook_tag\n"
#         "  • cta_tag\n"
#         "  • actor_tag\n"
#         "For ICP_TAGS, assign *at least one* and *at most two* tags:\n"
#         "  - If the ad clearly speaks to two audience segments, return both (comma-separated).\n"
#         "  - Only use “none” if absolutely no listed segment applies.\n\n"
#         "Return *only* a JSON object with these keys:\n"
#         "  { hierarchy_tag, storyline_tag, hook_tag, icp_tag, actor_tag }\n\n"
#         f"HIERARCHY_TAGS: {', '.join(HIERARCHY_TAGS)}\n"
#         f"STORYLINE_TAGS: {', '.join(STORYLINE_TAGS)}\n"
#         f"HOOK_TAGS: {', '.join(HOOK_TAGS)}\n"
#         f"ACTOR_TAGS: {', '.join(ACTOR_TAGS)}\n"
#         f"CTA_TAGS: {', '.join(CTA_TAGS)}\n"
#         f"ICP_TAGS (choose 1-2, minimize “none”): {', '.join(ICP_TAGS)}\n"
#     )


# def generate_tags(client, prompt: str) -> dict:
#     """Call Gemini model to generate tag JSON for a single prompt."""
#     resp = client.models.generate_content(
#         model=MODEL_NAME,
#         contents=prompt,
#         config=types.GenerateContentConfig(
#             system_instruction=(
#                 "You are a content-tagging assistant. "
#                 "Given video metadata, choose the best single tag from each provided list."
#             ),
#             temperature=0.0,
#             max_output_tokens=100,
#         ),
#     )
#     raw = resp.text.strip()
#     try:
#         return json.loads(raw)
#     except json.JSONDecodeError:
#         start, end = raw.find("{"), raw.rfind("}")
#         if start != -1 and end != -1:
#             return json.loads(raw[start:end+1])
#         return {key: 'none' for key in ['hierarchy_tag','storyline_tag','hook_tag','icp_tag','actor_tag']}


# def main():
#     client = genai.Client(api_key=API_KEY)
#     df = pd.read_csv(INPUT_FILE)

#     required_cols = ['snapshot.videos', 'snapshot.title', 'snapshot.body.text',
#                      'snapshot.page_categories', 'snapshot.link_description', 'snapshot.cta_type']
#     missing = [c for c in required_cols if c not in df.columns]
#     if missing:
#         raise ValueError(f"Missing required columns in CSV: {missing}")

#     results = []
#     for _, row in df.iterrows():
#         # extract HD video URL only
#         raw_video = row.get('snapshot.videos', None)
#         video_info = {}
#         if isinstance(raw_video, dict):
#             video_info = raw_video
#         elif isinstance(raw_video, str):
#             try:
#                 video_info = json.loads(raw_video)
#             except json.JSONDecodeError:
#                 video_info = {}
#         # handle NaN or floats by keeping video_info empty
#         url = video_info.get('video_hd_url') or video_info.get('video_sd_url') or ''

#         title = row['snapshot.title']
#         text = row['snapshot.body.text']
#         heirarchy = row['snapshot.page_categories']
#         description = row['snapshot.link_description']
#         cta_type = row['snapshot.cta_type']

#         prompt = prepare_prompt(url, title, text, heirarchy, description, cta_type)
#         tags = generate_tags(client, prompt)

#         entry = {**row.to_dict(), **tags}
#         results.append(entry)
#         time.sleep(0.1)

#     with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
#         json.dump(results, f, ensure_ascii=False, indent=2)

#     print(f"Tagged {len(results)} records and saved to {OUTPUT_FILE}")


# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
import os
import time
import json
import ast
from google import genai
from google.genai import types
from google.genai.errors import ClientError
import torch

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY     = os.getenv('GEMINI_API_KEY')
MODEL_NAME  = "gemini-2.0-flash-001"
INPUT_FILE  = "top40_sorted__meta_ads.json"
OUTPUT_DIR  = "tagged_sorted_meta_ads"
DEVICE       = "cuda" if torch.cuda.is_available() else "cpu"
MAX_RETRIES = 5
RETRY_DELAY  = 5
# ────────────────────────────────────────────────────────────────────────────────

# ─── TAG CATEGORIES ────────────────────────────────────────────────────────────
HIERARCHY_TAGS = ["product", "category", "industry", "brand", "none"]
STORYLINE_TAGS = [
    "unboxing","testimonial","before-after","tutorial","listicle",
    "daily-routine","voice-over-showcase","dialogue","replicate-ad",
    "demonstration","none"
]
HOOK_TAGS = [
    "strong-reaction","dramatize-problem","absurd-alternative",
    "visual-trick","highlight-popularity","target-audience-callout",
    "controversy","emphasize-one-usp","none"
]
CTA_TAGS = [
    "buy_now","download_now","visit_website","sign_up","subscribe",
    "start_free_trial","learn_more","none"
]
ICP_TAGS = ["moms","athletes","students","travelers","golfers"]
ACTOR_TAGS = ["male","female","mixed","none"]
# ────────────────────────────────────────────────────────────────────────────────



def prepare_prompt(video_url: str, title: str, text: str, cta: str) -> str:
    """Build the prompt instructing Gemini to tag and pick the best ICP persona."""
    return (
        f"Video URL: {video_url}\n"
        f"Title: {title}\n"
        f"Text: {text}\n"
        f"CTA Type: {cta}\n\n"
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
        except ClientError as e:
            attempts += 1
            print(f"Rate limit hit (attempt {attempts}/{MAX_RETRIES}), retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    # fallback default tags
    return {key: 'none' for key in ['hierarchy_tag','storyline_tag','hook_tag','cta_tag','actor_tag','icp_tag']}


def main():
    client = genai.Client(api_key=API_KEY)

    # Read raw file and split JSON objects by blank line
    raw = open(INPUT_FILE, 'r', encoding='utf-8').read()
    chunks = [c.strip() for c in raw.split('\n\n') if c.strip()]
    entries = []
    for chunk in chunks:
        try:
            entries.append(json.loads(chunk))
        except json.JSONDecodeError:
            try:
                entries.append(ast.literal_eval(chunk))
            except Exception:
                continue

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tagged_results = []

    for entry in entries:
        video_info = entry.get('snapshot.videos', {}) or {}
        if isinstance(video_info, str):
            try:
                video_info = json.loads(video_info)
            except Exception:
                try:
                    video_info = ast.literal_eval(video_info)
                except Exception:
                    video_info = {}
        video_url = video_info.get('video_hd_url') or video_info.get('video_sd_url') or ''

        title = entry.get('snapshot_title', '')
        text  = entry.get('snapshot_body_text', '')
        cta   = entry.get('snapshot_cta_type', 'none')

        prompt = prepare_prompt(video_url, title, text, cta)
        tags = generate_tags(client, prompt)

        combined = {**entry, **tags}
        tagged_results.append(combined)
        time.sleep(0.1)

    # Save master
    all_path = os.path.join(OUTPUT_DIR, 'all_tagged.json')
    with open(all_path, 'w', encoding='utf-8') as f:
        json.dump(tagged_results, f, ensure_ascii=False, indent=2)
    print(f"Saved all tagged entries to {all_path}")

    # Save per-persona
    for persona in ICP_TAGS:
        subset = [e for e in tagged_results if e.get('icp_tag', '').lower() == persona]
        if subset:
            path = os.path.join(OUTPUT_DIR, f"tagged_{persona}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(subset)} entries for persona '{persona}' to {path}")

if __name__ == "__main__":
    main()