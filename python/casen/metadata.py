"""
Metadata module for CASEN.

Provides variable search and value-label extraction without loading full datasets.
"""

import io
import re
import tempfile
import warnings
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pandas.io.stata import StataReader

from casen.downloader import CasenDownloader
from casen.utils import normalize_text, get_cached_metadata, save_cached_metadata


def search(pattern: str, years: Optional[List[int]] = None, regex: bool = False,
           verbose: bool = True) -> Dict[int, Dict[str, str]]:
    """
    Search for variables in CASEN datasets by pattern.

    Args:
        pattern: Search pattern (string or regex)
        years: List of years to search (None = most recent year only)
        regex: If True, treat pattern as regex; if False, as literal substring
        verbose: Print results to console

    Returns:
        Dictionary: {year: {var_name: var_label}}
    """
    downloader = CasenDownloader(verbose=False)

    if years is None:
        years = [max(downloader.AVAILABLE_YEARS)]

    all_results: Dict[int, Dict[str, str]] = {}

    for year in years:
        if verbose:
            print(f"\nBuscando en CASEN {year}...")

        metadata = _get_or_build_metadata(year, downloader, verbose)
        if metadata is None:
            if verbose:
                print(f"  [ERROR] No se pudo obtener metadata para {year}")
            continue

        matches = _search_in_metadata(metadata, pattern, regex)
        if verbose:
            _print_search_results(year, matches, pattern)

        if matches:
            all_results[year] = matches

    return all_results


def get_labels(variable: str, year: Optional[int] = None, verbose: bool = True) -> Optional[Dict]:
    """
    Get value labels (codebook) for a categorical variable.

    Args:
        variable: Variable name (e.g., "region", "ecivil")
        year: Survey year (None = most recent year)
        verbose: Print labels to console

    Returns:
        Dictionary mapping codes to labels, or None.
    """
    downloader = CasenDownloader(verbose=False)

    if year is None:
        year = max(downloader.AVAILABLE_YEARS)

    if verbose:
        print(f"\nObteniendo etiquetas para '{variable}' en CASEN {year}...")

    # Use metadata cache to validate variable existence without full data load.
    metadata = _get_or_build_metadata(year, downloader, verbose=False)
    if metadata is None:
        if verbose:
            print(f"  [ERROR] No se pudo obtener metadata para CASEN {year}")
        return None

    if variable not in metadata:
        if verbose:
            print(f"  [ERROR] Variable '{variable}' no existe en CASEN {year}")
            print(f"  Sugerencia: Use search('{variable}') para buscar variables similares")
        return None

    labels_dict = _extract_value_labels(year, variable, downloader, verbose)

    if labels_dict is None:
        if verbose:
            print(f"  [INFO] Variable '{variable}' no tiene etiquetas (variable continua)")
        return None

    if verbose:
        _print_value_labels(variable, labels_dict)

    return labels_dict


def _get_or_build_metadata(year: int, downloader: CasenDownloader, verbose: bool) -> Optional[Dict[str, str]]:
    """
    Get metadata from cache or download/build if missing.
    """
    metadata = get_cached_metadata(year)
    if metadata is not None:
        return metadata

    metadata = _extract_metadata(year, downloader, verbose)
    if metadata is not None:
        save_cached_metadata(year, metadata)

    return metadata


def _download_year_payload(year: int, downloader: CasenDownloader) -> Optional[io.BytesIO]:
    """
    Download raw file payload (.dta, .zip, etc.) for a given year.
    """
    original_verbose = downloader.verbose
    downloader.verbose = False

    try:
        best_url = downloader._fetch_best_url(year)
        if best_url is None:
            return None

        return downloader._download_file(best_url)
    finally:
        downloader.verbose = original_verbose


def _extract_dta_buffer(file_data: io.BytesIO, downloader: CasenDownloader, year: int,
                        verbose: bool = False) -> Optional[io.BytesIO]:
    """
    Extract and return a .dta buffer from raw payload using downloader logic.
    """
    original_verbose = downloader.verbose
    downloader.verbose = verbose

    try:
        return downloader.extract_dta_buffer(file_data, year)
    finally:
        downloader.verbose = original_verbose


def _read_metadata_pyreadstat(dta_buffer: io.BytesIO, verbose: bool = False) -> Optional[Dict[str, str]]:
    """
    Fallback metadata extraction using pyreadstat (supports older Stata versions).
    """
    try:
        import pyreadstat  # type: ignore
    except Exception:
        if verbose:
            print("  [ERROR] pyreadstat no disponible para fallback de metadata")
        return None

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".dta", delete=False) as tmp_file:
            dta_buffer.seek(0)
            tmp_file.write(dta_buffer.read())
            tmp_path = tmp_file.name

        _df, meta = pyreadstat.read_dta(
            tmp_path,
            metadataonly=True,
            apply_value_formats=False,
            formats_as_category=False,
        )

        names = list(meta.column_names or [])
        labels = list(meta.column_labels or [])
        result: Dict[str, str] = {}
        for idx, name in enumerate(names):
            label = labels[idx] if idx < len(labels) and labels[idx] is not None else ""
            result[name] = str(label)

        return result
    except Exception as e:
        if verbose:
            print(f"  [ERROR] Fallback pyreadstat metadata falló: {e}")
        return None
    finally:
        if tmp_path and Path(tmp_path).exists():
            try:
                Path(tmp_path).unlink()
            except Exception:
                pass


def _read_value_labels_pyreadstat(dta_buffer: io.BytesIO, variable: str,
                                  verbose: bool = False) -> Optional[Dict]:
    """
    Fallback value-label extraction using pyreadstat.
    """
    try:
        import pyreadstat  # type: ignore
    except Exception:
        if verbose:
            print("  [ERROR] pyreadstat no disponible para fallback de etiquetas")
        return None

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".dta", delete=False) as tmp_file:
            dta_buffer.seek(0)
            tmp_file.write(dta_buffer.read())
            tmp_path = tmp_file.name

        _df, meta = pyreadstat.read_dta(
            tmp_path,
            metadataonly=True,
            apply_value_formats=False,
            formats_as_category=False,
        )

        variable_to_label = dict(meta.variable_to_label or {})
        all_value_labels = dict(meta.value_labels or {})

        # Preferred path: variable -> labelset name -> values.
        labelset_name = variable_to_label.get(variable)
        if labelset_name:
            return all_value_labels.get(labelset_name)

        # Fallback for non-standard mappings.
        return all_value_labels.get(variable)
    except Exception as e:
        if verbose:
            print(f"  [ERROR] Fallback pyreadstat etiquetas falló: {e}")
        return None
    finally:
        if tmp_path and Path(tmp_path).exists():
            try:
                Path(tmp_path).unlink()
            except Exception:
                pass


def _extract_metadata(year: int, downloader: CasenDownloader, verbose: bool) -> Optional[Dict[str, str]]:
    """
    Extract variable_labels dictionary from .dta file (efficient).
    """
    file_data = _download_year_payload(year, downloader)
    if file_data is None:
        return None

    dta_buffer = _extract_dta_buffer(file_data, downloader, year, verbose=verbose)
    if dta_buffer is None:
        return None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            reader = pd.read_stata(dta_buffer, iterator=True)
            metadata = reader.variable_labels()
            reader.close()

        if verbose:
            print(f"  [OK] {len(metadata)} variables encontradas")

        return metadata
    except Exception as e:
        fallback = _read_metadata_pyreadstat(dta_buffer, verbose=verbose)
        if fallback is not None:
            if verbose:
                print(f"  [OK] {len(fallback)} variables encontradas (fallback pyreadstat)")
            return fallback

        if verbose:
            print(f"  [ERROR] {e}")
        return None


def _search_in_metadata(metadata: Dict[str, str], pattern: str, regex: bool) -> Dict[str, str]:
    """
    Search for pattern in variable names and labels.
    """
    matches: Dict[str, str] = {}
    normalized_pattern = normalize_text(pattern)

    if regex:
        try:
            compiled_pattern = re.compile(normalized_pattern, re.IGNORECASE)
        except re.error:
            regex = False

    for var_name, var_label in metadata.items():
        label_text = var_label if isinstance(var_label, str) else str(var_label or "")
        norm_var_name = normalize_text(var_name)
        norm_var_label = normalize_text(label_text)

        if regex:
            if compiled_pattern.search(norm_var_name) or compiled_pattern.search(norm_var_label):
                matches[var_name] = label_text
        else:
            if normalized_pattern in norm_var_name or normalized_pattern in norm_var_label:
                matches[var_name] = label_text

    return matches


def _print_search_results(year: int, matches: Dict[str, str], pattern: str) -> None:
    """
    Pretty-print search results to console.
    """
    if not matches:
        print(f"  No se encontraron variables con '{pattern}'")
        return

    print(f"  Encontradas {len(matches)} variables:")
    print("  " + "-" * 70)
    print(f"  {'Variable':<20} {'Descripcion':<50}")
    print("  " + "-" * 70)

    for i, (var_name, var_label) in enumerate(matches.items()):
        if i >= 50:
            print(f"  ... y {len(matches) - 50} mas (use results dict para ver todos)")
            break

        label_display = var_label[:47] + "..." if len(var_label) > 50 else var_label
        print(f"  {var_name:<20} {label_display:<50}")


def _extract_value_labels(year: int, variable: str, downloader: CasenDownloader,
                          verbose: bool) -> Optional[Dict]:
    """
    Extract value labels for a specific variable using StataReader metadata.
    """
    file_data = _download_year_payload(year, downloader)
    if file_data is None:
        return None

    dta_buffer = _extract_dta_buffer(file_data, downloader, year, verbose=verbose)
    if dta_buffer is None:
        return None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            reader = StataReader(dta_buffer)
            all_value_labels = reader.value_labels()
            # Pandas/Stata may store labels keyed by labelset name (e.g., labels123),
            # not by variable name. Map variable -> labelset through reader internals.
            varlist = list(getattr(reader, "_varlist", []))
            lbllist = list(getattr(reader, "_lbllist", []))
            reader.close()

        # Case 1: direct mapping by variable name
        direct = all_value_labels.get(variable)
        if direct is not None:
            return direct

        # Case 2: mapping through variable's labelset name
        if variable in varlist:
            var_idx = varlist.index(variable)
            if 0 <= var_idx < len(lbllist):
                labelset_name = lbllist[var_idx]
                if labelset_name:
                    return all_value_labels.get(labelset_name)

        return None
    except Exception as e:
        fallback = _read_value_labels_pyreadstat(dta_buffer, variable, verbose=verbose)
        if fallback is not None:
            return fallback

        if verbose:
            print(f"  [ERROR] {e}")
        return None


def _print_value_labels(variable: str, labels: Dict) -> None:
    """
    Pretty-print value labels to console.
    """
    print(f"  Etiquetas de '{variable}':")
    print("  " + "-" * 50)
    print(f"  {'Codigo':<10} {'Etiqueta':<40}")
    print("  " + "-" * 50)

    for code in sorted(labels.keys()):
        label = labels[code]
        label_display = label[:37] + "..." if len(label) > 40 else label
        print(f"  {code:<10} {label_display:<40}")
