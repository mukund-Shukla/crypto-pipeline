import requests
import json
import os
import time
import logging
import yaml
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path        # ← this is what was missing

# Loading configuration from YAML file
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Setting up logging

# This runs once when the module loads.
# Every other file will use logging.getLogger(__name__)
# and it automatically uses this same config.

logging.basicConfig(
    level=config["logging"]["level"],
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(config["logging"]["log_file"]),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_page(page_number:int)->list[dict]:  #Fetch a single page of market data from CoinGecko. Retries only this specific page on failure-not the entire dataset. Returns empty list if API returns no data for this page.
    url=f'{config["api"]["base_url"]}/coins/markets'
    params={
        "vs_currency":config["api"]["currency"],
        "order":"market_cap_desc",
        "per_page":config["api"]["per_page"],
        "page":page_number,
        "sparkline":False,
        "price_change_percentage":"1h,24h,7d"
    }

    attempts=config["api"]["retry_attempts"]
    delay=config["api"]["retry_delay_seconds"]

    for attempt in range(1, attempts + 1):
        try:
            logger.info(f"Fetching page {page_number} - attempt {attempt}/{attempts}")
            response=requests.get(url, params=params, timeout=10)
            
            #429 Too Many Requests - we hit the rate limit, so we wait and retry.
            if response.status_code==429:
                wait=delay*attempt*2
                logger.warning(f"Rate limit hit. Waiting {wait} seconds before retrying page {page_number}.")
                time.sleep(wait)
                continue

            response.raise_for_status() #raises exception is status>400
            page_data=response.json()

            #Empty page means we've fetched all available data, so we can stop.It should return empty list, and the main loop will break.
            if not page_data:
                logger.info(f"No data returned for page {page_number}. Assuming we've fetched all available coins.")
                return []
            logger.info(f"Successfully fetched page {page_number} with {len(page_data)} coins.")
            return page_data
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out on page {page_number}.")
        except requests.RequestException as e:
            logger.error(f"HTTP error on page {page_number}: {e}")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error on page {page_number} - API is down or something gone crazy with the network.")
        
        if attempt<attempts:
            logger.info(f"Retrying page {page_number} in {delay} seconds...")
            time.sleep(delay)
            delay*=2 # exponential backoff — wait longer each retry

        # If we exhaust all attempts for this page, we log an error and return empty list to skip this page.
    raise RuntimeError(f"Failed to fetch page {page_number} after multiple attempts.")

def fetch_market_data()->list[dict]:  #It will return a list of dicts, each dict is a coin's data as returned by the API.
    # Fetch top N coins by market cap from CoinGecko.
    # Retries on failure with exponential backoff.
    # Each page is retried independently — a failure on page 3 won't cause us to lose data from pages 1 and 2.
    # Returns raw list of dicts exactly as the API returned them. Stops when: target coin count reached OR API returns empty page.

    top_n = config["api"]["top_n"]
    per_page = config["api"]["per_page"]
    rate_limit_delay = config["api"]["rate_limit_delay_seconds"]

    # How many pages do we need? Ex. top_n=250, per_page=100 → ceil(250/100) = 3 pages

    import math
    total_pages=math.ceil(top_n/per_page)
    logger.info(f"Fetching top {top_n} coins from CoinGecko, which requires {total_pages} pages (per_page={per_page}).")
    all_coins=[]

    for page_num in range(1, total_pages + 1):
        page_data=fetch_page(page_num)
        if not page_data:  # If we get an empty page, we assume we've fetched all available data and break the loop.
            logger.info(f"Stopping fetch - no more data available")
            break
        all_coins.extend(page_data)
        logger.info(f"Total coins fetched so far: {len(all_coins)}")

        #Stopping if we have enoughh coins

        if len(all_coins)>=top_n:
            all_coins=all_coins[:top_n]  #Trim to exact top_n if we fetched a bit extra on the last page.
            logger.info(f"Reached target of {top_n} coins. Stopping fetch.")
            break

            
        if page_num<total_pages:  # Don't wait after the last page.
            logger.info(f"Waiting {rate_limit_delay} seconds before fetching next page to respect rate limits.")
            time.sleep(rate_limit_delay)


    logger.info(f"Finished fetching market data. Total coins fetched: {len(all_coins)}")
    return all_coins

# Save raw API response as JSON, partitioned by date.
# DOES NOT overwrite — if today's file already exists, skips and warns.
# Returns the path where file was saved, or None if skipped.

def save_raw_data(data:list[dict])->Path:  #The filename should include the path, e.g. "raw/market_data_2024-06-01.json".
    now=datetime.now()
    today=now.strftime("%Y-%m-%d")
    timestamp=now.strftime("%Y-%m-%dT%H%M%S")  # e.g. 20260329_143022
    partition_dir=Path(config["storage"]["raw_dir"])/f"date={today}"
    partition_dir.mkdir(parents=True, exist_ok=True)  #Create the directory if it doesn't exist.

    filepath =partition_dir/f"market_data_{timestamp}.json"

    #  No overwrite check 
    # If the file already exists, this run is a duplicate.
    # Log a warning and return — don't silently overwrite.

    if filepath.exists():
        logger.warning(f"Raw data file {filepath} already exists. Skipping save to avoid overwriting.")
        return filepath
    
    with open(filepath, 'w') as f:
        json.dump({
            "extracted_at": now.isoformat(),
            "record_count": len(data),
            "data": data
        },f, indent=2)

    logger.info(f"Saved raw data to {filepath} with {len(data)} records.")
    return filepath

if __name__=="__main__":
    try:
        market_data=fetch_market_data()
        path=save_raw_data(market_data)
    except Exception as e:
        logger.exception(f"An error occurred during extraction: {e}")