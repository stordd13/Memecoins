import os
import json
import time
import pandas as pd
import argparse
import logging
from datetime import datetime
from tqdm import tqdm
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from memecoin_utils import get_memecoins, get_coin_snapshot, get_coin_history

def setup_logging(output_dir):
    """Setup logging configuration"""
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'memecoin_pipeline_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_processed_coins(output_dir, frequencies):
    """Get list of already processed coins"""
    processed = {
        'snapshots': set(),
        'history': {freq: set() for freq in frequencies}
    }
    
    # Check snapshots
    snapshot_dir = os.path.join(output_dir, 'snapshots')
    if os.path.exists(snapshot_dir):
        for file in os.listdir(snapshot_dir):
            if file.endswith('_snapshot.json'):
                coin_id = file.replace('_snapshot.json', '')
                processed['snapshots'].add(coin_id)
    
    # Check history
    history_dir = os.path.join(output_dir, 'history')
    if os.path.exists(history_dir):
        for file in os.listdir(history_dir):
            for freq in frequencies:
                if file.endswith(f'_{freq}.parquet'):
                    coin_id = file.replace(f'_{freq}.parquet', '')
                    processed['history'][freq].add(coin_id)
    
    return processed

def retry_with_backoff(func, max_retries=5, initial_delay=1):
    """Retry a function with exponential backoff"""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func()
        except (requests.exceptions.RequestException, 
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError) as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

def main():
    parser = argparse.ArgumentParser(description="Memecoin Data Pipeline")
    parser.add_argument('-n', '--num', type=int, default=100, help='Number of memecoins to fetch')
    parser.add_argument('-f', '--frequencies', nargs='+', default=['daily', 'hourly', 'minute'],
                        help="Frequencies for history (choose from: daily, hourly, minute)")
    parser.add_argument('--output', type=str, default='data', help='Output directory')
    parser.add_argument('--resume', action='store_true', help='Resume from last checkpoint')
    parser.add_argument('--retry-delay', type=int, default=2, help='Delay between retries in seconds')
    args = parser.parse_args()

    N = args.num
    frequencies = args.frequencies
    output_dir = args.output
    retry_delay = args.retry_delay

    # Setup logging
    logger = setup_logging(output_dir)
    logger.info(f"Starting pipeline with parameters: N={N}, frequencies={frequencies}, output_dir={output_dir}")

    # Get list of already processed coins if resuming
    processed_coins = get_processed_coins(output_dir, frequencies) if args.resume else {
        'snapshots': set(),
        'history': {freq: set() for freq in frequencies}
    }
    
    if args.resume:
        logger.info("Resuming from checkpoint...")
        logger.info(f"Already processed snapshots: {len(processed_coins['snapshots'])}")
        for freq in frequencies:
            logger.info(f"Already processed {freq} history: {len(processed_coins['history'][freq])}")

    # 1. Get memecoins list
    logger.info("Fetching memecoins list...")
    num_pages = (N // 250) + 1
    memecoins_df = get_memecoins(num_pages=num_pages).head(N)
    os.makedirs(output_dir, exist_ok=True)
    memecoins_df.to_parquet(f'{output_dir}/memecoins_list.parquet')
    logger.info(f"Successfully fetched {len(memecoins_df)} memecoins")

    # 2. Get snapshots
    logger.info("Fetching snapshots...")
    os.makedirs(f'{output_dir}/snapshots', exist_ok=True)
    snapshot_errors = []
    
    remaining_coins = [coin_id for coin_id in memecoins_df['id'] if coin_id not in processed_coins['snapshots']]
    for coin_id in tqdm(remaining_coins, desc="Fetching snapshots"):
        try:
            snapshot = retry_with_backoff(lambda: get_coin_snapshot(coin_id))
            if snapshot:
                with open(f'{output_dir}/snapshots/{coin_id}_snapshot.json', 'w') as f:
                    json.dump(snapshot, f)
                logger.debug(f"Saved snapshot for {coin_id}")
            else:
                snapshot_errors.append(coin_id)
                logger.warning(f"Failed to get snapshot for {coin_id}")
        except Exception as e:
            snapshot_errors.append(coin_id)
            logger.error(f"Error processing snapshot for {coin_id}: {str(e)}")
        time.sleep(retry_delay)

    if snapshot_errors:
        logger.warning(f"Failed to get snapshots for {len(snapshot_errors)} coins: {snapshot_errors}")

    # 3. Get history
    logger.info("Fetching historical data...")
    os.makedirs(f'{output_dir}/history', exist_ok=True)
    history_errors = []

    for coin_id in tqdm(memecoins_df['id'], desc="Processing coins"):
        for freq in frequencies:
            if coin_id in processed_coins['history'][freq]:
                logger.debug(f"Skipping already processed {freq} data for {coin_id}")
                continue
                
            try:
                logger.debug(f"Fetching {freq} data for {coin_id}")
                hist_df = retry_with_backoff(lambda: get_coin_history(coin_id, frequency=freq))
                hist_df.to_parquet(f'{output_dir}/history/{coin_id}_{freq}.parquet')
                logger.debug(f"Saved {freq} data for {coin_id}")
            except Exception as e:
                history_errors.append((coin_id, freq))
                logger.error(f"Error fetching {freq} history for {coin_id}: {str(e)}")
            time.sleep(retry_delay)

    if history_errors:
        logger.warning(f"Failed to get history for {len(history_errors)} coin-frequency pairs")

    # Summary
    logger.info("Pipeline completed!")
    logger.info(f"Total memecoins processed: {len(memecoins_df)}")
    logger.info(f"Snapshots saved: {len(memecoins_df) - len(snapshot_errors)}")
    logger.info(f"History data saved: {len(memecoins_df) * len(frequencies) - len(history_errors)}")
    
    if snapshot_errors or history_errors:
        logger.warning("Some errors occurred during processing. Check the log file for details.")

if __name__ == '__main__':
    main() 