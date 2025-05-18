"""
Helper functions for the ML project.
"""
import os
import pickle
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Tuple, Union

def save_pickle(obj: Any, filepath: str) -> None:
    """
    Save an object as a pickle file.
    
    Args:
        obj: Object to save
        filepath: Path to save the pickle file
    """
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f)
    print(f"Object saved to {filepath}")

def load_pickle(filepath: str) -> Any:
    """
    Load an object from a pickle file.
    
    Args:
        filepath: Path to the pickle file
        
    Returns:
        The loaded object
    """
    with open(filepath, 'rb') as f:
        obj = pickle.load(f)
    print(f"Object loaded from {filepath}")
    return obj

def create_directory(directory: str) -> None:
    """
    Create a directory if it doesn't exist.
    
    Args:
        directory: Directory path
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")
