"""
CASEN - Modern Python Library for Chilean CASEN Survey Data

A professional, modular library for downloading and analyzing CASEN survey data.

Features:
- 100% in-memory operations (zero disk I/O)
- Intelligent URL scoring system (ported from MATA)
- Vectorized Stata 17 integration (100x speedup)
- Efficient variable search without loading full datasets
- Value label extraction (codebook access)

Basic Usage:
    >>> import casen
    >>> df = casen.download(2022)
    >>> results = casen.search("educacion")
    >>> labels = casen.get_labels("region", 2022)

Author: Maykol Medrano
License: MIT
"""

from casen.downloader import CasenDownloader
from casen.stata_io import to_stata, is_stata_available
from casen.metadata import search, get_labels
from casen.utils import normalize_text, clear_cache

from typing import Optional, Dict, List
import pandas as pd


# ============================================================================
# PUBLIC API - VERB-BASED NAMING (Industry Standard)
# ============================================================================

def download(year: int, to_stata: bool = True, verbose: bool = True) -> Optional[pd.DataFrame]:
    """
    Download CASEN survey data for a single year.

    This is the main entry point for the module. When imported as `import casen`,
    users can call `casen.download(2022)` for a clean, verb-based API.

    Args:
        year: Survey year (e.g., 1990, 2000, 2017, 2022, 2024)
        to_stata: If True, inject data to Stata using sfi.Data (requires Stata environment)
        verbose: Enable detailed progress messages

    Returns:
        DataFrame with survey data, or None if failed

    Examples:
        >>> import casen
        >>> df = casen.download(2022)
        >>> # Returns DataFrame with 202,231 rows × 918 columns

        >>> # Download without Stata injection
        >>> df = casen.download(2022, to_stata=False)

        >>> # Quiet mode
        >>> df = casen.download(2022, verbose=False)

    Available years:
        1990, 1992, 1994, 1996, 1998, 2000, 2003, 2006, 2009,
        2011, 2013, 2015, 2017, 2022, 2024
    """
    downloader = CasenDownloader(verbose=verbose)
    df = downloader.download_casen(year)

    if df is not None and to_stata:
        to_stata(df)

    return df


def download_batch(years: List[int], to_stata: bool = True, verbose: bool = True) -> Dict[int, pd.DataFrame]:
    """
    Download CASEN survey data for multiple years (batch processing).

    Args:
        years: List of years to download
        to_stata: If True, inject LAST successful download to Stata
        verbose: Enable detailed progress messages

    Returns:
        Dictionary mapping year -> DataFrame

    Examples:
        >>> import casen
        >>> results = casen.download_batch([2017, 2020, 2022])
        >>> # Returns: {2017: DataFrame, 2020: DataFrame, 2022: DataFrame}

        >>> # Download multiple years without Stata injection
        >>> results = casen.download_batch([2015, 2017, 2022], to_stata=False)

        >>> # Access individual DataFrames
        >>> df_2022 = results[2022]
    """
    downloader = CasenDownloader(verbose=verbose)
    return downloader.download_multiple(years, load_to_stata=to_stata)


# ============================================================================
# LEGACY ALIASES (Backward Compatibility - Will be deprecated in v2.0)
# ============================================================================

def download_casen_year(year: int, load_to_stata: bool = True) -> Optional[pd.DataFrame]:
    """
    DEPRECATED: Use download() instead.

    Legacy alias for backward compatibility.
    Will be removed in version 2.0.
    """
    import warnings
    warnings.warn(
        "download_casen_year() is deprecated, use download() instead",
        DeprecationWarning,
        stacklevel=2
    )
    return download(year, to_stata=load_to_stata)


def download_casen_multiple(years: List[int], load_last: bool = True) -> Dict[int, pd.DataFrame]:
    """
    DEPRECATED: Use download_batch() instead.

    Legacy alias for backward compatibility.
    Will be removed in version 2.0.
    """
    import warnings
    warnings.warn(
        "download_casen_multiple() is deprecated, use download_batch() instead",
        DeprecationWarning,
        stacklevel=2
    )
    return download_batch(years, to_stata=load_last)


# ============================================================================
# PACKAGE METADATA
# ============================================================================

__version__ = "1.0.0"
__author__ = "Maykol Medrano"
__license__ = "MIT"

__all__ = [
    # Main API
    'download',
    'download_batch',
    'search',
    'get_labels',

    # Classes
    'CasenDownloader',

    # Stata integration
    'to_stata',
    'is_stata_available',

    # Utilities
    'normalize_text',
    'clear_cache',

    # Legacy (deprecated)
    'download_casen_year',
    'download_casen_multiple',
]
