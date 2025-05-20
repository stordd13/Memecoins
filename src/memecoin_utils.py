from pycoingecko import CoinGeckoAPI
import pandas as pd
import time
from tqdm import tqdm
from utils import retry_with_backoff

def get_memecoins(num_pages=1):
    cg = CoinGeckoAPI()
    all_memecoins = []

    for page in tqdm(range(1, num_pages + 1), desc="Fetching pages from CoinGecko", dynamic_ncols=True):
        def call():
            return cg.get_coins_markets(
                vs_currency='usd',
                category='meme-token',
                order='market_cap_desc',
                per_page=250,
                page=page,
                sparkline=False
            )
        try:
            memecoins = retry_with_backoff(call, max_retries=3, initial_delay=1.2)
        except Exception as e:
            print(f"[ERROR] Page {page} failed permanently: {e}")
            continue

        if not memecoins:
            break

        all_memecoins.extend(memecoins)
        # time.sleep(1.2)

    return pd.DataFrame(all_memecoins)


def get_coin_snapshot(coin_id):
    cg = CoinGeckoAPI()
    try:
        return cg.get_coin_by_id(coin_id)
    except Exception as e:
        print(f"Error fetching snapshot for {coin_id}: {e}")
        return None

def get_coin_history(coin_id, frequency='daily'):
    cg = CoinGeckoAPI()
    if frequency == 'minute':
        days = 7
        interval = None
    elif frequency == 'hourly':
        days = 90
        interval = None
    elif frequency == 'daily':
        days = 365
        interval = 'daily'
    else:
        raise ValueError("frequency must be 'minute', 'hourly', or 'daily'")
    kwargs = dict(id=coin_id, vs_currency='usd', days=days)
    if interval is not None:
        kwargs['interval'] = interval
    market_data = cg.get_coin_market_chart_by_id(**kwargs)
    prices_df = pd.DataFrame(market_data['prices'], columns=['timestamp', 'price'])
    market_caps_df = pd.DataFrame(market_data['market_caps'], columns=['timestamp', 'market_cap'])
    volumes_df = pd.DataFrame(market_data['total_volumes'], columns=['timestamp', 'volume'])
    for df in [prices_df, market_caps_df, volumes_df]:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    final_df = prices_df.merge(market_caps_df, on='timestamp').merge(volumes_df, on='timestamp')
    return final_df 