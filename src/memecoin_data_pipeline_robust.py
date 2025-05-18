import os
import json
import time
import pandas as pd
import argparse
from tqdm import tqdm
from memecoin_utils import get_memecoins, get_coin_history
from utils import setup_logging, retry_with_backoff, get_processed_files

def main():
    parser = argparse.ArgumentParser(description="Memecoin Data Pipeline (History Only)")
    parser.add_argument('-n', '--num', type=int, default=100, help='Number of memecoins to fetch (-1 for all)')
    parser.add_argument('-f', '--frequencies', nargs='+', default=['daily'], help="Frequencies to fetch")
    parser.add_argument('--output', type=str, default='data', help='Output directory')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--retry-delay', type=int, default=2)
    parser.add_argument('--max-retries', type=int, default=5)
    args = parser.parse_args()

    N = args.num
    frequencies = args.frequencies
    output_dir = args.output
    retry_delay = args.retry_delay
    max_retries = args.max_retries

    # Setup logging
    logger = setup_logging(output_dir, 'memecoin_pipeline')
    logger.info(f"Starting pipeline: N={N}, frequencies={frequencies}, output_dir={output_dir}")

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

    # Fetch memecoin list
    logger.info("Fetching memecoin list from CoinGecko")
    if N == -1:
        num_pages = 100  # fetch up to 25,000 tokens
    else:
        num_pages = (N // 250) + 1

    memecoins_df = get_memecoins(num_pages=num_pages)
    if N != -1:
        memecoins_df = memecoins_df.head(N)

    memecoins_df["fetch_rank"] = memecoins_df.index
    memecoins_df["fetched_at"] = pd.Timestamp.utcnow()
    os.makedirs(output_dir, exist_ok=True)
    memecoins_df.to_parquet(f'{output_dir}/memecoins_list.parquet')
    logger.info(f"Saved {len(memecoins_df)} memecoins to memecoins_list.parquet")

    # Fetch historical data
    os.makedirs(f'{output_dir}/history', exist_ok=True)
    history_errors = []
    insufficient_data = []

    for coin_id in tqdm(memecoins_df['id'], desc="Fetching historical data"):
        for freq in frequencies:
            if coin_id in processed_history[freq]:
                continue
            try:
                hist_df = retry_with_backoff(
                    lambda: get_coin_history(coin_id, frequency=freq),
                    max_retries=max_retries,
                    initial_delay=retry_delay,
                    logger=logger
                )

                if hist_df is not None and not hist_df.empty and hist_df.shape[0] >= 3:
                    hist_df.to_parquet(f'{output_dir}/history/{coin_id}_{freq}.parquet')
                else:
                    insufficient_data.append(coin_id)
                    logger.warning(f"Insufficient or empty {freq} data for {coin_id}")
            except Exception as e:
                history_errors.append((coin_id, freq))
                logger.error(f"Error fetching {freq} history for {coin_id}: {str(e)}")
            time.sleep(retry_delay)

    # Save failed coin IDs
    if history_errors or insufficient_data:
        with open(f"{output_dir}/missing_history.txt", "w") as f:
            for coin_id in set(x[0] for x in history_errors) | set(insufficient_data):
                f.write(f"{coin_id}\n")

    logger.info(f"Successfully fetched history for {len(memecoins_df) - len(insufficient_data) - len(history_errors)} coins")
    logger.info(f"Missing/invalid history: {len(insufficient_data)}")
    logger.info(f"Errors during fetch: {len(history_errors)}")

if __name__ == '__main__':
    main()
