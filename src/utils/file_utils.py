import os
from pathlib import Path
import polars as pl
from tqdm import tqdm
from typing import Union, List

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

def get_relevant_parquet_files(folder_path: Union[str, Path], min_size: int = 5000) -> List[Path]:
    """
    Get all parquet files that are not empty and have sufficient data.
    
    Args:
        folder_path: Path to the folder containing parquet files
        min_size: Minimum file size in bytes (default: 5000)
        
    Returns:
        List of Path objects for relevant parquet files
    """
    folder = Path(folder_path)
    files = list(folder.glob("*.parquet"))
    relevant_files = []
    
    for file in tqdm(files, desc="Filtering files"):
        if os.path.getsize(file) < min_size:  # Skip small files
            continue
        try:
            df = pl.read_parquet(file)
            if df.height < 3:  # Skip files with insufficient data
                continue
            relevant_files.append(file)
        except Exception:
            continue
            
    print(f"Number of relevant files: {len(relevant_files)}")
    return relevant_files 