import os

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