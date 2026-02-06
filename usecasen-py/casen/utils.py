"""
Utility functions for CASEN library.

Provides text normalization, caching, and helper functions.
"""

import unicodedata
import pickle
from pathlib import Path
from typing import Optional, Dict, Any


# Cache directory for metadata
CACHE_DIR = Path.home() / ".casen_cache"


def normalize_text(text: str) -> str:
    """
    Normalize text for case-insensitive and accent-insensitive search.

    Removes diacritics (tildes) and converts to lowercase.
    Examples:
        'Región' -> 'region'
        'Educación' -> 'educacion'
        'SALUD' -> 'salud'

    Args:
        text: Input text to normalize

    Returns:
        Normalized text (lowercase, no accents)
    """
    # Decompose Unicode characters (é -> e + ´)
    nfd = unicodedata.normalize('NFD', text)

    # Filter out combining characters (accents)
    without_accents = ''.join(
        char for char in nfd
        if unicodedata.category(char) != 'Mn'
    )

    # Convert to lowercase
    return without_accents.lower()


def get_cached_metadata(year: int) -> Optional[Dict[str, str]]:
    """
    Retrieve cached variable metadata for a given year.

    Args:
        year: Survey year

    Returns:
        Dictionary mapping var_name -> var_label, or None if not cached
    """
    if not CACHE_DIR.exists():
        return None

    cache_file = CACHE_DIR / f"metadata_{year}.pkl"

    if cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception:
            # Corrupted cache, ignore
            return None

    return None


def save_cached_metadata(year: int, metadata: Dict[str, str]) -> None:
    """
    Save variable metadata to cache.

    Args:
        year: Survey year
        metadata: Dictionary mapping var_name -> var_label
    """
    # Create cache directory if needed
    CACHE_DIR.mkdir(exist_ok=True)

    cache_file = CACHE_DIR / f"metadata_{year}.pkl"

    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(metadata, f)
    except Exception:
        # Silently fail if caching doesn't work
        pass


def clear_cache() -> None:
    """
    Clear all cached metadata files.
    """
    if CACHE_DIR.exists():
        for cache_file in CACHE_DIR.glob("metadata_*.pkl"):
            try:
                cache_file.unlink()
            except Exception:
                pass
