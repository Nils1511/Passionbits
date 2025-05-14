#!/usr/bin/env python3
"""
Scoring and aggregating Facebook ads data.

This script processes filtered ad data, scores them based on various
metrics, and selects the top performing ads.
"""

import pandas as pd
import json
import ast
import os


def load_input_data(input_path='filtered_meta_ads.json'):
    """
    Load JSON data from a pretty-formatted input file.
    
    Args:
        input_path: Path to input JSON file
        
    Returns:
        List of record dictionaries
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
        
    print(f"Loading data from {input_path}...")
    
    # Try to read as a single JSON array first
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        # If that fails, try reading as multiple JSON objects separated by newlines
        text = open(input_path, 'r', encoding='utf-8').read()
        decoder = json.JSONDecoder()
        records = []
        idx = 0
        length = len(text)

        while idx < length:
            try:
                while idx < length and text[idx].isspace():
                    idx += 1
                if idx >= length:
                    break
                obj, end = decoder.raw_decode(text, idx)
                records.append(obj)
                idx = end
            except json.JSONDecodeError:
                # Skip this line and move to the next one
                idx = text.find('\n', idx) + 1
                if idx <= 0:
                    break
        
        print(f"Loaded {len(records)} records")
        return records


def process_videos(df):
    """
    Process video information in the DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with processed video information
    """
    # Handle different formats of video data
    def parse_video_field(video_field):
        if isinstance(video_field, dict):
            return video_field
        elif isinstance(video_field, str):
            try:
                return ast.literal_eval(video_field)
            except (ValueError, SyntaxError):
                try:
                    return json.loads(video_field)
                except json.JSONDecodeError:
                    return {}
        return {}

    # Convert snapshot.videos string to dict
    df['snapshot.videos'] = df['snapshot.videos'].apply(parse_video_field)

    # Extract video_hd_url
    df['video_hd_url'] = df['snapshot.videos'].apply(
        lambda d: d.get('video_hd_url', d.get('video_sd_url', '')).strip() if isinstance(d, dict) else ''
    )

    # Drop rows with empty video_hd_url
    df = df[df['video_hd_url'] != ""]
    
    return df


def aggregate_by_video(df):
    """
    Group by video URL and aggregate data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Aggregated DataFrame
    """
    # Make sure all required columns exist, using empty strings for missing ones
    columns_to_aggregate = [
        'page_name', 'snapshot.page_profile_picture_url', 'snapshot.body.text',
        'snapshot.caption', 'snapshot.cta_text', 'snapshot.cta_type',
        'snapshot.link_description', 'snapshot.link_url', 'snapshot.page_categories',
        'snapshot.page_like_count', 'snapshot.title'
    ]
    
    # Check if columns exist and add empty placeholders if not
    for col in columns_to_aggregate:
        if col not in df.columns:
            df[col] = ''
    
    # Rename columns to make aggregation easier
    renamed_df = df.rename(columns={
        'page_name': 'page_name',
        'snapshot.page_profile_picture_url': 'snapshot_page_profile_picture_url',
        'snapshot.body.text': 'snapshot_body_text',
        'snapshot.caption': 'snapshot_caption',
        'snapshot.cta_text': 'snapshot_cta_text',
        'snapshot.cta_type': 'snapshot_cta_type',
        'snapshot.link_description': 'snapshot_link_description',
        'snapshot.link_url': 'snapshot_link_url',
        'snapshot.page_categories': 'snapshot_page_categories',
        'snapshot.page_like_count': 'snapshot_page_like_count',
        'snapshot.title': 'snapshot_title',
    })
    
    # Determine which columns actually exist in the DataFrame
    agg_columns = {col.replace('.', '_'): col for col in columns_to_aggregate if col in df.columns}
    
    # Build the aggregation dictionary
    agg_dict = {
        'video_hd_url': 'count',
        'days_since_start': 'max'
    }
    
    # Add the first values for each column
    for renamed_col in [
        'page_name', 'snapshot_page_profile_picture_url', 'snapshot_body_text',
        'snapshot_caption', 'snapshot_cta_text', 'snapshot_cta_type',
        'snapshot_link_description', 'snapshot_link_url', 'snapshot_page_categories',
        'snapshot_page_like_count', 'snapshot_title'
    ]:
        if renamed_col in renamed_df.columns:
            agg_dict[renamed_col] = 'first'
    
    # Add video preview image URL aggregation if the column exists
    agg_dict['snapshot.videos'] = lambda d: d.iloc[0].get('video_preview_image_url', '') \
        if 'snapshot.videos' in renamed_df.columns else ''
    
    # Group by video URL and aggregate
    try:
        agg = renamed_df.groupby('video_hd_url').agg(**{
            'count': ('video_hd_url', 'size'),
            'days_since_start': ('days_since_start', 'max'),
            'page_name': ('page_name', 'first'),
            'snapshot_page_profile_picture_url': ('snapshot_page_profile_picture_url', 'first'),
            'snapshot_body_text': ('snapshot_body_text', 'first'),
            'snapshot_caption': ('snapshot_caption', 'first'),
            'snapshot_cta_text': ('snapshot_cta_text', 'first'),
            'snapshot_cta_type': ('snapshot_cta_type', 'first'),
            'snapshot_link_description': ('snapshot_link_description', 'first'),
            'snapshot_link_url': ('snapshot_link_url', 'first'),
            'snapshot_page_categories': ('snapshot_page_categories', 'first'),
            'snapshot_page_like_count': ('snapshot_page_like_count', 'first'),
            'snapshot_title': ('snapshot_title', 'first'),
            'video_preview_image_url': ('snapshot.videos', lambda d: d.iloc[0].get('video_preview_image_url', '') if isinstance(d.iloc[0], dict) else '')
        }).reset_index()
    except Exception as e:
        print(f"Error in aggregation: {e}")
        # Fallback to a simpler aggregation
        agg = renamed_df.groupby('video_hd_url').agg({
            'video_hd_url': 'size',
            'days_since_start': 'max',
        }).rename(columns={'video_hd_url': 'count'}).reset_index()
        
        # Add back the other columns by taking the first value in each group
        for col in renamed_df.columns:
            if col not in agg.columns and col != 'video_hd_url':
                try:
                    first_values = renamed_df.groupby('video_hd_url')[col].first()
                    agg = agg.merge(first_values.to_frame(), left_on='video_hd_url', right_index=True)
                except Exception:
                    # Skip columns that cause issues
                    continue
    
    return agg


def sort_and_select_top(agg, top_n=40):
    """
    Sort ads and select top performing ones.
    
    Args:
        agg: Aggregated DataFrame
        top_n: Number of top ads to select
        
    Returns:
        DataFrame with top ads
    """
    # Check if required columns exist
    sort_columns = []
    if 'days_since_start' in agg.columns:
        sort_columns.append('days_since_start')
    if 'count' in agg.columns:
        sort_columns.append('count')
    if 'snapshot_page_like_count' in agg.columns:
        sort_columns.append('snapshot_page_like_count')
    
    # If we have columns to sort by, do it
    if sort_columns:
        # Sort with all columns in descending order
        agg = agg.sort_values(
            by=sort_columns,
            ascending=[False] * len(sort_columns)
        )

    # Keep top N ads only
    return agg.head(top_n)


def finalize_data(df):
    """
    Finalize data structure for output.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Finalized DataFrame
    """
    # Check if required columns exist
    if 'video_hd_url' in df.columns and 'video_preview_image_url' in df.columns:
        # Recreate snapshot.videos dict
        df['snapshot.videos'] = df.apply(lambda r: {
            'video_hd_url': r['video_hd_url'],
            'video_preview_image_url': r['video_preview_image_url']
        }, axis=1)

        # Drop helper columns
        df.drop(columns=['video_hd_url', 'video_preview_image_url'], inplace=True)
    
    return df


def save_to_json(df, output_path='sorted_meta_ads.json'):
    """
    Save DataFrame to a pretty-printed JSON file.
    
    Args:
        df: Input DataFrame
        output_path: Path to output JSON file
    """
    # with open(output_path, 'w', encoding='utf-8') as fout:
    #     cnt = 0
    #     for rec in df.to_dict(orient='records'):
    #         json.dump(rec, fout, ensure_ascii=False, indent=4)
    #         fout.write('\n\n')
    #         cnt += 1
            
    # print(f"Top {cnt} ads saved to: {output_path}")
    records = df.to_dict(orient='records')

    with open(output_path, 'w', encoding='utf-8') as fout:
        json.dump(records, fout, ensure_ascii=False, indent=4)
    print(f"Saved {len(records)} filtered data as JSON array to {output_path}")


def main():
    """Main function to orchestrate the scoring workflow."""
    try:
        # Step 1: Load data from input file
        records = load_input_data()
        
        # Step 2: Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Step 3: Process video information
        df = process_videos(df)
        print(f"Processing {len(df)} ads with valid videos")
        
        # Step 4: Group by video URL and aggregate
        agg = aggregate_by_video(df)
        print(f"Found {len(agg)} unique videos")
        
        # Step 5: Sort and select top ads
        top_ads = sort_and_select_top(agg)
        print(f"Selected top {len(top_ads)} ads")
        
        # Step 6: Finalize data structure
        top_ads = finalize_data(top_ads)
        
        # Step 7: Save to JSON
        save_to_json(top_ads)
        
        return True
        
    except Exception as e:
        print(f"Error in sorting.py: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()