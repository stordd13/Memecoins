import os
import logging
from datetime import datetime
import time
import requests

def setup_logging(output_dir, script_name):
    """Setup logging configuration"""
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'{script_name}_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

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

def get_processed_files(output_dir, file_pattern, logger=None):
    """Get list of already processed files based on pattern"""
    processed = set()
    
    if os.path.exists(output_dir):
        files = [f for f in os.listdir(output_dir) if f.endswith(file_pattern)]
        for file in files:
            # Extract the identifier (e.g., coin_id) from the filename
            # Assumes pattern is something like "id_pattern.ext"
            identifier = file.replace(file_pattern, '')
            processed.add(identifier)
        if logger:
            logger.info(f"Found {len(files)} existing files in {output_dir}")
    
    return processed 