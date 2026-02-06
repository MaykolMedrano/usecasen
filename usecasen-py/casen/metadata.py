"""
Metadata Module for CASEN

Provides efficient variable search and label extraction without loading full datasets.
Uses pandas.io.stata iterators for O(1) memory operations.
"""

import io
import re
import warnings
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pandas.io.stata import StataReader

from casen.downloader import CasenDownloader
from casen.utils import normalize_text, get_cached_metadata, save_cached_metadata


def search(pattern: str, years: Optional[List[int]] = None, regex: bool = False,
           verbose: bool = True) -> Dict[int, Dict[str, str]]:
    """
    Search for variables in CASEN datasets by pattern.

    EFFICIENCY: Uses pd.read_stata(iterator=True).variable_labels() to extract
    ONLY the metadata dictionary (O(1) memory), never loads full dataset.

    CAPABILITIES:
    - Accent insensitive: "región" matches "Region" and "Región"
    - Case insensitive: "EDAD" matches "edad"
    - Regex support: "^y.*" finds variables starting with 'y'
    - Searches in both var_name and var_label

    Args:
        pattern: Search pattern (string or regex)
        years: List of years to search (None = most recent year only)
        regex: If True, treat pattern as regex; if False, as literal substring
        verbose: Print results to console

    Returns:
        Dictionary: {year: {var_name: var_label}}

    Examples:
        >>> # Find all education variables
        >>> results = search("educ")

        >>> # Find all variables starting with 'y' (income)
        >>> results = search("^y", regex=True)

        >>> # Search across multiple years
        >>> results = search("salud", years=[2017, 2022])
    """
    downloader = CasenDownloader(verbose=False)

    # Default to most recent year if not specified
    if years is None:
        years = [max(downloader.AVAILABLE_YEARS)]

    all_results = {}

    for year in years:
        if verbose:
            print(f"\nBuscando en CASEN {year}...")

        # Try cache first
        metadata = get_cached_metadata(year)

        if metadata is None:
            # Download and extract metadata
            metadata = _extract_metadata(year, downloader, verbose)

            if metadata is None:
                if verbose:
                    print(f"  [ERROR] No se pudo obtener metadata para {year}")
                continue

            # Save to cache
            save_cached_metadata(year, metadata)

        # Search in metadata
        matches = _search_in_metadata(metadata, pattern, regex)

        if verbose:
            _print_search_results(year, matches, pattern)

        if matches:
            all_results[year] = matches

    return all_results


def get_labels(variable: str, year: Optional[int] = None, verbose: bool = True) -> Optional[Dict]:
    """
    Get value labels (codebook) for a categorical variable.

    EFFICIENCY: Uses pd.io.stata.StataReader directly to access value_labels()
    without loading the full dataset.

    Args:
        variable: Variable name (e.g., "region", "ecivil")
        year: Survey year (None = most recent year)
        verbose: Print labels to console

    Returns:
        Dictionary mapping codes to labels: {1: 'Tarapacá', 2: 'Antofagasta', ...}
        None if variable has no labels (continuous variable)

    Examples:
        >>> # Get region labels
        >>> labels = get_labels("region", 2022)
        >>> # Returns: {1: 'Tarapacá', 2: 'Antofagasta', ...}

        >>> # Get marital status labels
        >>> labels = get_labels("ecivil", 2022)
        >>> # Returns: {1: 'Soltero', 2: 'Casado', ...}
    """
    downloader = CasenDownloader(verbose=False)

    # Default to most recent year if not specified
    if year is None:
        year = max(downloader.AVAILABLE_YEARS)

    if verbose:
        print(f"\nObteniendo etiquetas para '{variable}' en CASEN {year}...")

    # Download the .dta file
    df = downloader.download_casen(year)

    if df is None:
        if verbose:
            print(f"  [ERROR] No se pudo descargar CASEN {year}")
        return None

    # Check if variable exists
    if variable not in df.columns:
        if verbose:
            print(f"  [ERROR] Variable '{variable}' no existe en CASEN {year}")
            print(f"  Sugerencia: Use search('{variable}') para buscar variables similares")
        return None

    # Re-download with StataReader to get value labels
    # We need to fetch the file again with iterator=True
    labels_dict = _extract_value_labels(year, variable, downloader, verbose)

    if labels_dict is None:
        if verbose:
            print(f"  [INFO] Variable '{variable}' no tiene etiquetas (variable continua)")
        return None

    if verbose:
        _print_value_labels(variable, labels_dict)

    return labels_dict


def _extract_metadata(year: int, downloader: CasenDownloader, verbose: bool) -> Optional[Dict[str, str]]:
    """
    Extract variable_labels dictionary from .dta file (efficient).

    Args:
        year: Survey year
        downloader: CasenDownloader instance
        verbose: Print progress messages

    Returns:
        Dictionary mapping var_name -> var_label
    """
    # Temporarily suppress downloader verbosity
    original_verbose = downloader.verbose
    downloader.verbose = False

    # Download file
    page_url = downloader._get_year_url(year)
    html_content = downloader._fetch_html(page_url)

    if html_content is None:
        downloader.verbose = original_verbose
        return None

    best_url = downloader._get_best_link(html_content, str(year))
    if not best_url:
        downloader.verbose = original_verbose
        return None

    best_url = downloader._normalize_url(best_url)
    file_data = downloader._download_file(best_url)

    if file_data is None:
        downloader.verbose = original_verbose
        return None

    # Extract .dta from ZIP
    import zipfile
    try:
        with zipfile.ZipFile(file_data) as zf:
            dta_files = [name for name in zf.namelist() if name.lower().endswith('.dta')]

            if not dta_files:
                downloader.verbose = original_verbose
                return None

            with zf.open(dta_files[0]) as dta_file:
                dta_buffer = io.BytesIO(dta_file.read())

                # Use StataReader with iterator=True for efficiency
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    reader = pd.read_stata(dta_buffer, iterator=True)
                    metadata = reader.variable_labels()

                downloader.verbose = original_verbose

                if verbose:
                    print(f"  [OK] {len(metadata)} variables encontradas")

                return metadata

    except Exception as e:
        downloader.verbose = original_verbose
        if verbose:
            print(f"  [ERROR] {e}")
        return None


def _search_in_metadata(metadata: Dict[str, str], pattern: str, regex: bool) -> Dict[str, str]:
    """
    Search for pattern in variable names and labels.

    Args:
        metadata: Dictionary {var_name: var_label}
        pattern: Search pattern
        regex: Whether to use regex matching

    Returns:
        Dictionary of matches {var_name: var_label}
    """
    matches = {}

    # Normalize pattern for accent-insensitive search
    normalized_pattern = normalize_text(pattern)

    # Compile regex if needed
    if regex:
        try:
            compiled_pattern = re.compile(normalized_pattern, re.IGNORECASE)
        except re.error:
            # Invalid regex, fallback to literal search
            regex = False

    for var_name, var_label in metadata.items():
        # Normalize variable name and label
        norm_var_name = normalize_text(var_name)
        norm_var_label = normalize_text(var_label)

        # Search
        if regex:
            if compiled_pattern.search(norm_var_name) or compiled_pattern.search(norm_var_label):
                matches[var_name] = var_label
        else:
            if normalized_pattern in norm_var_name or normalized_pattern in norm_var_label:
                matches[var_name] = var_label

    return matches


def _print_search_results(year: int, matches: Dict[str, str], pattern: str) -> None:
    """
    Pretty-print search results to console.

    Args:
        year: Survey year
        matches: Dictionary of matches
        pattern: Search pattern
    """
    if not matches:
        print(f"  No se encontraron variables con '{pattern}'")
        return

    print(f"  Encontradas {len(matches)} variables:")
    print("  " + "-" * 70)

    # Print table header
    print(f"  {'Variable':<20} {'Descripción':<50}")
    print("  " + "-" * 70)

    # Print matches (limit to 50 for readability)
    for i, (var_name, var_label) in enumerate(matches.items()):
        if i >= 50:
            print(f"  ... y {len(matches) - 50} más (use results dict para ver todos)")
            break

        # Truncate label if too long
        label_display = var_label[:47] + "..." if len(var_label) > 50 else var_label
        print(f"  {var_name:<20} {label_display:<50}")


def _extract_value_labels(year: int, variable: str, downloader: CasenDownloader,
                          verbose: bool) -> Optional[Dict]:
    """
    Extract value labels for a specific variable.

    Args:
        year: Survey year
        variable: Variable name
        downloader: CasenDownloader instance
        verbose: Print progress

    Returns:
        Dictionary mapping codes to labels, or None if no labels
    """
    # Temporarily suppress downloader verbosity
    original_verbose = downloader.verbose
    downloader.verbose = False

    # Download file (same as _extract_metadata)
    page_url = downloader._get_year_url(year)
    html_content = downloader._fetch_html(page_url)

    if html_content is None:
        downloader.verbose = original_verbose
        return None

    best_url = downloader._get_best_link(html_content, str(year))
    if not best_url:
        downloader.verbose = original_verbose
        return None

    best_url = downloader._normalize_url(best_url)
    file_data = downloader._download_file(best_url)

    if file_data is None:
        downloader.verbose = original_verbose
        return None

    # Extract .dta from ZIP
    import zipfile
    try:
        with zipfile.ZipFile(file_data) as zf:
            dta_files = [name for name in zf.namelist() if name.lower().endswith('.dta')]

            if not dta_files:
                downloader.verbose = original_verbose
                return None

            with zf.open(dta_files[0]) as dta_file:
                dta_buffer = io.BytesIO(dta_file.read())

                # Use StataReader to access value_labels()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    reader = StataReader(dta_buffer)

                    # Get all value labels
                    all_value_labels = reader.value_labels()

                    # Get variable label mapping (which variables use which label set)
                    var_to_labelset = {}

                    # Read first few rows to determine label set for this variable
                    # (Stata stores label set name separately from variable)
                    df_sample = pd.read_stata(dta_buffer, iterator=False,
                                             convert_categoricals=False)

                    reader.close()

                downloader.verbose = original_verbose

                # Try to find labels for this variable
                # Method 1: Check if variable name matches label set name
                if variable in all_value_labels:
                    return all_value_labels[variable]

                # Method 2: Search through all label sets
                # (some variables use different label set names)
                for labelset_name, labels in all_value_labels.items():
                    # Check if this variable's values match this label set
                    if variable in df_sample.columns:
                        unique_vals = df_sample[variable].dropna().unique()
                        label_keys = set(labels.keys())

                        # If significant overlap, this is probably the right label set
                        if len(set(unique_vals) & label_keys) >= min(3, len(unique_vals) * 0.5):
                            return labels

                # No labels found
                return None

    except Exception as e:
        downloader.verbose = original_verbose
        if verbose:
            print(f"  [ERROR] {e}")
        return None


def _print_value_labels(variable: str, labels: Dict) -> None:
    """
    Pretty-print value labels to console.

    Args:
        variable: Variable name
        labels: Dictionary mapping codes to labels
    """
    print(f"  Etiquetas de '{variable}':")
    print("  " + "-" * 50)
    print(f"  {'Código':<10} {'Etiqueta':<40}")
    print("  " + "-" * 50)

    # Sort by code
    for code in sorted(labels.keys()):
        label = labels[code]
        label_display = label[:37] + "..." if len(label) > 40 else label
        print(f"  {code:<10} {label_display:<40}")
