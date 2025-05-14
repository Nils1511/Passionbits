#!/usr/bin/env python3
"""
Filtering and preprocessing Facebook ads data.

This script processes raw JSON ad data, cleans it, and filters it 
based on specific criteria.
"""

import json
import pandas as pd
from datetime import datetime


def load_and_flatten_json(input_json='poloshirts_meta_ads.json'):
    """
    Load JSON data and flatten nested structures.
    
    Args:
        input_json: Path to input JSON file
        
    Returns:
        pandas DataFrame with flattened data
    """
    print(f"Loading data from {input_json}...")
    with open(input_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Flatten nested structures
    df = pd.json_normalize(data, sep='.')
    
    # Drop columns with all missing values
    df.dropna(axis=1, how='all', inplace=True)
    
    return df


def clean_data(df):
    """
    Clean the dataframe by handling dates and missing values.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    # Convert Unix timestamps to datetime for start_date and end_date
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')

    # Handle missing values
    # For numeric columns, fill missing with 0
    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(0)

    # For object/string columns, fill missing with empty string
    obj_cols = df.select_dtypes(include=['object']).columns
    df[obj_cols] = df[obj_cols].fillna('')

    # Example: if there are list columns you may want to convert them to strings
    list_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
    for col in list_cols:
        df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else x)
        
    return df


def add_time_metrics(df):
    """
    Add time-based metrics like days_since_start.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with added time metrics
    """
    # Ensure 'start_date' is in datetime format
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')

    # Calculate days since start
    today = pd.Timestamp.today()
    df['days_since_start'] = (today - df['start_date']).dt.days
    
    return df


def filter_data(df):
    """
    Filter data based on specific criteria.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Filtered DataFrame
    """
    columns_to_keep = [
        'page_name', 'snapshot.page_profile_picture_url', 'snapshot.body.text',
        'snapshot.caption', 'snapshot.cta_text', 'snapshot.cta_type', 
        'snapshot.link_description', 'snapshot.link_url', 'snapshot.page_categories',
        'snapshot.page_like_count', 'snapshot.title', 'snapshot.videos',
        'days_since_start'
    ]

    # Filter the DataFrame to only include selected columns
    # Use columns that exist in the df (handle case where some might be missing)
    valid_columns = [col for col in columns_to_keep if col in df.columns]
    meta = df[valid_columns]

    # Drop rows where snapshot.videos is NaN
    if 'snapshot.videos' in meta.columns:
        meta = meta.dropna(subset=['snapshot.videos'])

        # Drop rows where snapshot.videos is "empty"
        meta = meta[meta['snapshot.videos'].apply(lambda vids: bool(vids))]

    # Additional filtering criteria
    if 'days_since_start' in meta.columns and 'snapshot.page_like_count' in meta.columns:
        return meta[(meta['days_since_start'] >= 14) & (meta['snapshot.page_like_count'] > 18000)]
    else:
        print("Warning: Required columns for filtering not found. Returning unfiltered data.")
        return meta


def save_to_json(df, output_json='filtered_meta_ads.json'):
    """
    Save DataFrame to a pretty-printed JSON file.
    
    Args:
        df: Input DataFrame
        output_json: Path to output JSON file
    """
    records = df.to_dict(orient='records')
    
    # with open(output_json, 'w', encoding='utf-8') as fout:
    #     for rec in records:
    #         json.dump(rec, fout, ensure_ascii=False, indent=4)
    #         fout.write('\n')  # blank line between records
            
    # print(f"Saved filtered data to {output_json}")
    with open(output_json, 'w', encoding='utf-8') as fout:
        json.dump(records, fout, ensure_ascii=False, indent=4)
    print(f"Saved filtered data as JSON array to {output_json}")


def main():
    """Main function to orchestrate the data processing workflow."""
    try:
        # Step 1: Load and flatten JSON
        df = load_and_flatten_json()
        print(f"Loaded data with {len(df)} rows and {len(df.columns)} columns")
        
        # Step 2: Clean the data
        df = clean_data(df) 
        
        # Step 5: Add time metrics
        df = add_time_metrics(df)
        print("Added time metrics")
        
        # Step 6: Filter data
        filtered_df = filter_data(df)
        print(f"Filtered data down to {len(filtered_df)} rows")
        
        # Convert to a list of dicts (one dict per row)
        records = filtered_df.to_dict(orient='records')

       # Save to pretty JSON (this is the input for the scoring step)
        save_to_json(filtered_df)  # Call save_to_json directly here

        print(f"Filtered {len(filtered_df)} data saved to filtered_meta_ads.json")
        return True

        
    except Exception as e:
        print(f"Error in filtering.py: {str(e)}")
        return False


if __name__ == "__main__":
    main()


