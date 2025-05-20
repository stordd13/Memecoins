import os
import time
import json
import argparse
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from memecoin_utils import get_memecoins, get_coin_history
from utils import setup_logging, retry_with_backoff, get_processed_files


def fetch_and_save(coin_id, freq, output_dir, retry_delay, max_retries, logger):
    try:
        df = retry_with_backoff(
            lambda: get_coin_history(coin_id, frequency=freq),
            max_retries=max_retries,
            initial_delay=retry_delay,
            logger=logger
        )
        if df is not None and not df.empty and df.shape[0] >= 3:
            output_path = os.path.join(output_dir, 'history', f'{coin_id}_{freq}.parquet')
            df.to_parquet(output_path)
            return True, coin_id
        else:
            logger.warning(f"Insufficient or empty {freq} data for {coin_id}")
            return False, coin_id
    except Exception as e:
        logger.error(f"Error fetching {freq} history for {coin_id}: {str(e)}")
        return False, coin_id


def main():
    parser = argparse.ArgumentParser(description="Parallel Memecoin History Fetcher")
    parser.add_argument('-n', '--num', type=int, default=1000)
    parser.add_argument('-f', '--frequencies', nargs='+', default=['daily'])
    parser.add_argument('--output', type=str, default='data')
    parser.add_argument('--threads', type=int, default=5, help='Number of threads (max ~10)')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--retry-delay', type=float, default=1.2)
    parser.add_argument('--max-retries', type=int, default=5)
    args = parser.parse_args()

    N = args.num
    frequencies = args.frequencies
    output_dir = args.output
    max_retries = args.max_retries
    retry_delay = args.retry_delay
    threads = args.threads

    logger = setup_logging(output_dir, 'memecoin_parallel')

    # Create folders
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'history'), exist_ok=True)

    # Fetch memecoins
    if N == -1:
        num_pages = 100
    else:
        num_pages = (N // 250) + 1

    memecoins_df = get_memecoins(num_pages=num_pages)
    if N != -1:
        memecoins_df = memecoins_df.head(N)

    memecoins_df["fetch_rank"] = memecoins_df.index
    memecoins_df["fetched_at"] = pd.Timestamp.utcnow()
    memecoins_df.to_parquet(os.path.join(output_dir, 'memecoins_list.parquet'))

    # Resume logic
    if args.resume:
        processed_history = {
            freq: get_processed_files(
                os.path.join(output_dir, 'history'),
                f'_{freq}.parquet',
                logger
            )
            for freq in frequencies
        }
    else:
        processed_history = {freq: set() for freq in frequencies}

    # Prepare tasks
    tasks = []
    for coin_id in memecoins_df['id']:
        for freq in frequencies:
            if coin_id in processed_history[freq]:
                continue
            tasks.append((coin_id, freq))

    logger.info(f"Total tasks to process: {len(tasks)}")

    # Run in thread pool
    results = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(fetch_and_save, coin_id, freq, output_dir, retry_delay, max_retries, logger): (coin_id, freq)
            for coin_id, freq in tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching memecoins", dynamic_ncols=True):
            success, coin_id = future.result()
            results.append((coin_id, success))

    # Save missing/failed coins
    failed_ids = [coin_id for coin_id, success in results if not success]
    if failed_ids:
        with open(os.path.join(output_dir, 'missing_history.txt'), 'w') as f:
            for coin_id in sorted(set(failed_ids)):
                f.write(f"{coin_id}\n")

    logger.info(f"Completed fetch. Successful: {len(results) - len(failed_ids)} / {len(results)}")

if __name__ == '__main__':
    main()
