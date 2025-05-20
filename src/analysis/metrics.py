import polars as pl
import numpy as np

def compute_metrics(df: pl.DataFrame) -> dict:
    """
    Compute basic metrics for a memecoin DataFrame.
    
    Args:
        df: DataFrame with price data
        
    Returns:
        Dictionary containing basic metrics
    """
    df = df.sort("timestamp")
    prices = df["price"].to_numpy()
    
    # Compute returns and volatility
    log_returns = np.diff(np.log(prices))
    ret = (prices[-1] - prices[0]) / prices[0]
    vol = np.std(log_returns)
    
    # Compute maximum drawdown
    running_max = np.maximum.accumulate(prices)
    drawdowns = (prices - running_max) / running_max
    max_dd = drawdowns.min()
    
    return {
        "return": ret,
        "volatility": vol,
        "max_drawdown": max_dd
    }

def extract_features(df: pl.DataFrame, early_days: int = 3, full_days: list = [30, 90, 180, 365]) -> dict:
    """
    Extract features from a memecoin DataFrame.
    
    Args:
        df: DataFrame with price, market_cap, and volume data
        early_days: Number of days to use for early features
        full_days: List of days to use for full period features
        
    Returns:
        Dictionary of features or None if insufficient data
    """
    df = df.sort("timestamp")
    if df.height < early_days:
        return None

    prices = df["price"].to_numpy()
    market_caps = df["market_cap"].to_numpy()
    volumes = df["volume"].to_numpy()

    result = {"symbol": None}
    eps = 1e-9  # To avoid divide-by-zero

    # --- Early features ---
    early_df = df.head(early_days)
    if early_df.height >= early_days:
        early_prices = early_df["price"].to_numpy()
        early_volumes = early_df["volume"].to_numpy()
        early_market_caps = early_df["market_cap"].to_numpy()
        early_log_returns = np.diff(np.log(early_prices))
        early_returns = np.diff(early_prices) / early_prices[:-1]

        result.update({
            "early_return": (early_prices[-1] - early_prices[0]) / (early_prices[0] + eps),
            "early_volatility": np.std(early_log_returns),
            "early_sharpe": np.mean(early_log_returns) / (np.std(early_log_returns) + eps),
            "early_marketcap_growth": (early_market_caps[-1] - early_market_caps[0]) / (early_market_caps[0] + eps),
            "early_volume_growth": (early_volumes[-1] - early_volumes[0]) / (early_volumes[0] + eps),
            "early_avg_volume": np.mean(early_volumes),
            "early_positive_days": np.sum(early_returns > 0),
        })

    # --- Full period features ---
    for d in full_days:
        if df.height >= d:
            d_prices = df.head(d)["price"].to_numpy()
            d_log_returns = np.diff(np.log(d_prices))
            d_returns = np.diff(d_prices) / d_prices[:-1]
            d_volumes = df.head(d)["volume"].to_numpy()
            d_market_caps = df.head(d)["market_cap"].to_numpy()

            running_max = np.maximum.accumulate(d_prices)
            drawdowns = (d_prices - running_max) / running_max
            max_dd = drawdowns.min()

            result.update({
                f"return_{d}d": (d_prices[-1] - d_prices[0]) / (d_prices[0] + eps),
                f"volatility_{d}d": np.std(d_log_returns),
                f"sharpe_{d}d": np.mean(d_log_returns) / (np.std(d_log_returns) + eps),
                f"max_drawdown_{d}d": max_dd,
                f"volume_growth_{d}d": (d_volumes[-1] - d_volumes[0]) / (d_volumes[0] + eps),
                f"marketcap_growth_{d}d": (d_market_caps[-1] - d_market_caps[0]) / (d_market_caps[0] + eps),
            })

    # --- Clean and fill problematic values ---
    clean_result = {}
    for k, v in result.items():
        if isinstance(v, float):
            if not np.isfinite(v) or np.isnan(v):
                clean_result[k] = 0.0  # or -999 if you want sentinel values
            else:
                clean_result[k] = v
        else:
            clean_result[k] = v  # keep symbol or other non-floats

    return clean_result 