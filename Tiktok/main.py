#!/usr/bin/env python3
"""
Facebook Ads Data Pipeline

This script orchestrates the complete pipeline for processing Facebook ads:
1. Scrape ads from Facebook using Apify
2. Filter and preprocess the raw data
3. Score and select top-performing ads
4. Tag the selected ads with content categories
"""

import os
import sys
import importlib.util
import time

def load_module_from_file(file_path, module_name):
    """
    Dynamically load a Python module from a file path.
    
    Args:
        file_path: Path to the Python file
        module_name: Name to give to the module
        
    Returns:
        Loaded module object
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def run_stage(name, module_path, function_name, success_message, failure_message):
    """
    Run a stage of the pipeline.
    
    Args:
        name: Name of the stage for logging
        module_path: Path to the Python file containing the code
        function_name: Name of the function to call
        success_message: Message to print on success
        failure_message: Message to print on failure
        
    Returns:
        True if the stage succeeded, False otherwise
    """
    print(f"\n{'=' * 80}")
    print(f"STAGE: {name}")
    print(f"{'=' * 80}")
    
    try:
        # Load the module
        module = load_module_from_file(module_path, f"pipeline_{name.lower().replace(' ', '_')}")
        
        # Get the function to call
        func = getattr(module, function_name)
        
        # Call the function
        start_time = time.time()
        result = func()
        elapsed_time = time.time() - start_time
        
        # Check the result
        if result:
            print(success_message)
            print(f"Completed in {elapsed_time:.2f} seconds")
            return True
        else:
            print(failure_message)
            return False
            
    except Exception as e:
        print(f"Error running {name}: {str(e)}")
        return False

def main():
    """Main function to run the complete Facebook ads pipeline."""
    stages = [
        # {
        #     "name": "Scraping",
        #     "module_path": "scraping.py",
        #     "function_name": "main",
        #     "success_message": "✅ Successfully scraped Facebook ads",
        #     "failure_message": "❌ Failed to scrape Facebook ads"
        # },
        {
            "name": "Filtering",
            "module_path": "filtering.py",
            "function_name": "main",
            "success_message": "✅ Successfully filtered ads data",
            "failure_message": "❌ Failed to filter ads data"
        },
        {
            "name": "Sorting",
            "module_path": "sorting.py",
            "function_name": "main",
            "success_message": "✅ Successfully scored and selected top ads",
            "failure_message": "❌ Failed to score ads"
        },
        # {
        #     "name": "Tagging",
        #     "module_path": "tagging.py",
        #     "function_name": "main",
        #     "success_message": "✅ Successfully tagged ads content",
        #     "failure_message": "❌ Failed to tag ads content"
        # }
    ]

    # Create a modified version of the scraping.py file with main() function
    create_modified_scraping_module()
    
    # Run each stage in sequence
    success_count = 0
    for stage in stages:
        if run_stage(**stage):
            success_count += 1
        else:
            print(f"\n⚠️ Stage {stage['name']} failed. Continuing with next stage...")
    
    # Print summary
    print(f"\n{'=' * 80}")
    print(f"PIPELINE SUMMARY: {success_count}/{len(stages)} stages completed successfully")
    print(f"{'=' * 80}")
    
    if success_count == len(stages):
        print("🎉 Complete pipeline executed successfully!")
    else:
        print(f"⚠️ Pipeline completed with {len(stages) - success_count} failed stages")

def create_modified_scraping_module():
    """
    Create a modified version of scraping.py with a main() function 
    that matches our pipeline pattern.
    """
    with open("scraping.py", "r") as f:
        content = f.read()
    
    # If the file doesn't already have a main() function that returns True/False
    if "def main():" not in content:
        with open("scraping.py", "w") as f:
            # Split on "if __name__ == \"__main__\":"
            parts = content.split('if __name__ == "__main__":')
            
            # Add our main function
            new_content = parts[0] + '''
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
'''
            f.write(new_content)

if __name__ == "__main__":
    main()