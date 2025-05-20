import time
import requests

def retry_with_backoff(func, max_retries=5, initial_delay=1, logger=None):
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
            if logger:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff 