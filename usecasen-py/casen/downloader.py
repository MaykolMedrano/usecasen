"""
CASEN Downloader Module

Handles web scraping, file download, and data extraction for CASEN surveys.
Implements intelligent URL scoring system ported from MATA.
"""

import io
import zipfile
from typing import Optional, Dict
import warnings

import pandas as pd
import requests
from tqdm import tqdm


class CasenDownloader:
    """
    Modern Python implementation of CASEN survey downloader.

    Features:
    - 100% in-memory operations (zero disk I/O)
    - Intelligent URL scoring system (ported from MATA)
    - Robust HTML parsing with retry logic
    - Progress bars for downloads
    """

    BASE_DOMAIN = "https://observatorio.ministeriodesarrollosocial.gob.cl"

    # Available years of CASEN survey
    AVAILABLE_YEARS = [1990, 1992, 1994, 1996, 1998, 2000, 2003, 2006, 2009,
                       2011, 2013, 2015, 2017, 2022, 2024]

    def __init__(self, timeout: int = 30, chunk_size: int = 8192, verbose: bool = True):
        """
        Initialize the downloader.

        Args:
            timeout: HTTP request timeout in seconds
            chunk_size: Download chunk size in bytes
            verbose: Enable detailed error messages
        """
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })

    def test_connectivity(self) -> bool:
        """
        Test connectivity to CASEN server.

        Returns:
            True if server is reachable, False otherwise
        """
        try:
            response = self.session.get(self.BASE_DOMAIN, timeout=5)
            return response.status_code == 200
        except:
            return False

    def _get_year_url(self, year: int) -> str:
        """
        Get the correct URL pattern for a given year.

        Available years: 1990, 1992, 1994, 1996, 1998, 2000, 2003, 2006, 2009,
                        2011, 2013, 2015, 2017, 2022, 2024

        Special case:
        - 2020: "encuesta-casen-en-pandemia-2020" (COVID special edition, may not be available)

        Standard pattern: "encuesta-casen-{year}"
        """
        if year == 2020:
            # Special COVID edition (files may be outdated/moved)
            return f"{self.BASE_DOMAIN}/encuesta-casen-en-pandemia-{year}"
        else:
            return f"{self.BASE_DOMAIN}/encuesta-casen-{year}"

    def download_casen(self, year: int) -> Optional[pd.DataFrame]:
        """
        Download and parse CASEN survey for a given year.

        Args:
            year: Survey year (e.g., 2017, 2020, 2022)

        Returns:
            DataFrame with survey data, or None if failed
        """
        print(f"  {year}: Buscando enlace...")

        # Step 1: Fetch HTML page (with year-specific URL pattern)
        page_url = self._get_year_url(year)
        html_content = self._fetch_html(page_url)

        if html_content is None:
            print(f"  {year}: [ERROR] No se pudo acceder a la web")
            return None

        # Step 2: Intelligent parsing with scoring system (MATA logic)
        best_url = self._get_best_link(html_content, str(year))

        if not best_url:
            print(f"  {year}: [ERROR] No se encontró archivo de datos")
            return None

        # Step 3: Normalize URL
        best_url = self._normalize_url(best_url)

        # Step 4: Download file (in-memory)
        print(f"  {year}: Descargando...")
        file_data = self._download_file(best_url)

        if file_data is None:
            print(f"  {year}: [ERROR] Falló la descarga")
            return None

        # Step 5: Extract and load .dta (in-memory)
        print(f"  {year}: Procesando archivo...")
        df = self._extract_and_load_dta(file_data, year)

        if df is None:
            print(f"  {year}: [ERROR] No se encontró .dta en el archivo")
            return None

        print(f"  {year}: [OK] {len(df):,} observaciones cargadas")
        return df

    def download_multiple(self, years: list[int], load_to_stata: bool = False) -> Dict[int, pd.DataFrame]:
        """
        Download multiple years of CASEN data.

        Args:
            years: List of years to download
            load_to_stata: Whether to inject last successful download to Stata

        Returns:
            Dictionary mapping year -> DataFrame
        """
        results = {}
        last_df = None

        print("=" * 60)
        print("  USECASEN Python v1.0 - Encuesta CASEN")
        print("=" * 60)
        print(f"Años: {years}")

        # Test connectivity
        if self.verbose:
            print("\nProbando conectividad al servidor...")
            if self.test_connectivity():
                print("  [OK] Servidor accesible")
            else:
                print("  [WARNING] Servidor no responde - las descargas pueden fallar")
                print("  [SUGERENCIA] Verifique su conexión a Internet")

        print()

        for year in years:
            df = self.download_casen(year)
            if df is not None:
                results[year] = df
                last_df = df

        print("=" * 60)

        # Load to Stata if requested (requires stata_io module)
        if load_to_stata and last_df is not None:
            from casen.stata_io import to_stata
            print("Cargando última base a Stata...")
            to_stata(last_df)

        return results

    def _fetch_html(self, url: str, retries: int = 3) -> Optional[str]:
        """
        Fetch HTML content from URL with retry logic.

        Args:
            url: URL to fetch
            retries: Number of retry attempts

        Returns:
            HTML content or None if all retries fail
        """
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout, verify=True)
                response.raise_for_status()
                return response.text

            except requests.exceptions.SSLError as e:
                if self.verbose:
                    print(f"       [SSL Error] Intento {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    if self.verbose:
                        print(f"       [DIAGNÓSTICO] Problema de certificado SSL")
                        print(f"       [SUGERENCIA] El sitio puede tener problemas de seguridad")
                    return None

            except requests.exceptions.Timeout as e:
                if self.verbose:
                    print(f"       [Timeout] Intento {attempt + 1}/{retries}: Servidor no responde")
                if attempt == retries - 1:
                    if self.verbose:
                        print(f"       [DIAGNÓSTICO] El servidor tardó más de {self.timeout}s")
                        print(f"       [SUGERENCIA] Intente aumentar el timeout")
                    return None

            except requests.exceptions.ConnectionError as e:
                if self.verbose:
                    print(f"       [Conexión] Intento {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    if self.verbose:
                        print(f"       [DIAGNÓSTICO] No se puede conectar al servidor")
                        print(f"       [SUGERENCIA] Verifique su conexión a Internet")
                    return None

            except requests.exceptions.HTTPError as e:
                if self.verbose:
                    print(f"       [HTTP {response.status_code}] {e}")
                    if response.status_code == 404:
                        print(f"       [DIAGNÓSTICO] La página no existe para ese año")
                        print(f"       [URL] {url}")
                    elif response.status_code == 403:
                        print(f"       [DIAGNÓSTICO] Acceso prohibido (firewall/anti-bot)")
                    elif response.status_code >= 500:
                        print(f"       [DIAGNÓSTICO] Error del servidor")
                return None

            except requests.RequestException as e:
                if self.verbose:
                    print(f"       [Error] Intento {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    return None

        return None

    def _get_best_link(self, html_content: str, year: str) -> Optional[str]:
        """
        MATA PORT: Intelligent link extraction with scoring system (IMPROVED).

        Enhanced version with better URL extraction for modern HTML.
        Implements the same scoring rules as MATA:
        - +100 for "stata"
        - +80 for ".dta"
        - +50 for year match
        - -100 for "spss"/"sav"
        - -60 for "manual"/"libro"

        Improvements over MATA:
        - Handles URLs with spaces
        - Extracts longer paths (storage/docs/...)
        - Better delimiter detection

        Args:
            html_content: Raw HTML string
            year: Survey year as string

        Returns:
            Best matching URL or None
        """
        # Normalize to lowercase (like MATA strlower)
        content = html_content.lower()

        best_url = None
        max_score = -9999

        # Extensions to search for (prioritize .dta direct, then compressed)
        extension_patterns = [
            '.dta',         # Direct .dta files (e.g., CASEN 2024)
            '.dta.zip',
            '.sav.zip',
            '.dta.rar',
            '.sav.rar',
            '.zip',
            '.rar',
        ]

        for ext in extension_patterns:
            # Find all occurrences of the extension
            pos = 0
            while True:
                ext_pos = content.find(ext, pos)
                if ext_pos == -1:
                    break

                # Back-trace to find URL start
                # IMPROVED: Search up to 500 chars back and look for path patterns
                candidate = None
                search_start = max(0, ext_pos - 500)

                # Try to find storage/docs pattern first (common in CASEN)
                storage_pos = content.rfind('storage/', search_start, ext_pos)
                if storage_pos != -1:
                    end_pos = ext_pos + len(ext)
                    candidate = content[storage_pos:end_pos]
                else:
                    # Fallback to delimiter-based extraction
                    start_delimiters = ['"', "'", '=', '>', '(', '\n', '\r']

                    for j in range(ext_pos, search_start, -1):
                        if content[j] in start_delimiters:
                            start_pos = j + 1
                            end_pos = ext_pos + len(ext)
                            candidate = content[start_pos:end_pos]
                            break

                # Validate and score
                if candidate and len(candidate) > 10:
                    # Clean up the candidate
                    candidate = candidate.strip()

                    # Skip if it's a fragment like "stata.dta.zip" without path
                    if not candidate.startswith('http') and '/' not in candidate and len(candidate) < 30:
                        pos = ext_pos + 1
                        continue

                    score = self._calculate_score(candidate, year)

                    if score > max_score:
                        max_score = score
                        best_url = candidate

                pos = ext_pos + 1

        return best_url

    def _calculate_score(self, url: str, year: str) -> int:
        """
        SCORING SYSTEM (exact port from MATA).

        Awards points for desired patterns, subtracts for undesired ones.

        Scoring rules:
        - Awards:
          - 'casen': +30
          - 'stata': +100
          - '.dta': +80
          - year match: +50
          - 'storage/docs/casen': +40
        - Penalties:
          - 'spss' / '.sav': -100
          - 'sas': -80
          - 'csv': -50
          - 'manual' / 'libro' / 'metodologia': -60
          - 'codigos' / 'cuestionario': -40

        Args:
            url: URL to score
            year: Survey year as string

        Returns:
            Score (higher is better)
        """
        score = 0

        # Awards
        if 'casen' in url:
            score += 30
        if 'stata' in url:
            score += 100
        if '.dta' in url:
            score += 80
        if year in url:
            score += 50
        if 'storage/docs/casen' in url:
            score += 40

        # Penalties
        if 'spss' in url:
            score -= 100
        if '.sav' in url:
            score -= 100
        if 'sas' in url:
            score -= 80
        if 'csv' in url:
            score -= 50
        if 'manual' in url:
            score -= 60
        if 'libro' in url:
            score -= 60
        if 'metodologia' in url:
            score -= 60
        if 'codigos' in url:
            score -= 40
        if 'cuestionario' in url:
            score -= 40

        return score

    def _normalize_url(self, url: str) -> str:
        """
        Normalize relative URLs to absolute.

        All CASEN files use: observatorio.ministeriodesarrollosocial.gob.cl
        """
        url = url.strip()

        if url.startswith('http'):
            return url

        # Storage files and all resources use same domain
        if url.startswith('storage/'):
            return f"{self.BASE_DOMAIN}/{url}"

        if url.startswith('/'):
            return f"{self.BASE_DOMAIN}{url}"

        return f"{self.BASE_DOMAIN}/{url}"

    def _try_url_variants(self, url: str) -> Optional[str]:
        """
        Try different capitalization variants of a URL.

        HTML often has lowercase, but files may use Title Case or UPPERCASE.
        Returns the first working URL or None.
        """
        # Generate variants
        variants = [url]

        # If URL contains storage/docs/casen, try title case variant
        if 'storage/docs/casen/' in url.lower():
            # Extract filename portion
            parts = url.split('/')
            filename = parts[-1]

            # Common patterns in CASEN files
            title_case_replacements = {
                'base de datos casen': 'Base de datos Casen',
                'casen': 'Casen',
                'stata': 'STATA',
                'spss': 'SPSS',
            }

            title_filename = filename
            for old, new in title_case_replacements.items():
                title_filename = title_filename.replace(old, new)

            title_url = '/'.join(parts[:-1] + [title_filename])
            if title_url != url:
                variants.append(title_url)

        # Try each variant with HEAD request
        for variant in variants:
            try:
                test_url = variant.replace(' ', '%20')
                response = self.session.head(test_url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    if self.verbose:
                        if variant != url:
                            print(f"       [INFO] Usando variante: {variant}")
                    return variant
            except:
                continue

        return None

    def _download_file(self, url: str, retries: int = 2) -> Optional[io.BytesIO]:
        """
        Download file to memory (100% RAM, ZERO disk) with retry logic.

        Args:
            url: File URL to download
            retries: Number of retry attempts

        Returns:
            BytesIO buffer or None
        """
        # Try to find working URL variant (handles capitalization issues)
        working_url = self._try_url_variants(url)

        if working_url is None:
            if self.verbose:
                print(f"       [ERROR] No se encontró variante válida de la URL")
            return None

        url = working_url

        for attempt in range(retries):
            try:
                # Handle spaces in URL
                url = url.replace(' ', '%20')

                response = self.session.get(url, stream=True, timeout=self.timeout * 2)
                response.raise_for_status()

                # Get total size for progress bar
                total_size = int(response.headers.get('content-length', 0))

                # Download to memory
                buffer = io.BytesIO()

                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc='       Descarga',
                             disable=not self.verbose) as pbar:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                buffer.write(chunk)
                                pbar.update(len(chunk))
                else:
                    # No content-length header, download without progress
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            buffer.write(chunk)

                buffer.seek(0)
                return buffer

            except requests.exceptions.Timeout as e:
                if self.verbose:
                    print(f"       [Timeout descarga] Intento {attempt + 1}/{retries}")
                if attempt == retries - 1:
                    if self.verbose:
                        print(f"       [DIAGNÓSTICO] El archivo es muy grande o conexión lenta")
                    return None

            except requests.exceptions.HTTPError as e:
                if self.verbose:
                    print(f"       [HTTP Error] {response.status_code}: {e}")
                    if response.status_code == 404:
                        print(f"       [DIAGNÓSTICO] El archivo ya no existe en el servidor")
                        print(f"       [URL] {url}")
                return None

            except requests.RequestException as e:
                if self.verbose:
                    print(f"       [Error descarga] Intento {attempt + 1}/{retries}: {e}")
                if attempt == retries - 1:
                    return None

        return None

    def _extract_and_load_dta(self, file_data: io.BytesIO, year: int) -> Optional[pd.DataFrame]:
        """
        Extract .dta from ZIP/RAR (in-memory) or load directly, then convert to DataFrame.

        Args:
            file_data: BytesIO buffer with compressed file or direct .dta
            year: Survey year (for error messages)

        Returns:
            DataFrame or None
        """
        # First, try to load as direct .dta file (CASEN 2024+)
        file_data.seek(0)  # Reset position
        try:
            if self.verbose:
                print(f"       [INFO] Intentando cargar como .dta directo...")

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_stata(file_data, convert_categoricals=False)

            if self.verbose:
                print(f"       [INFO] Archivo .dta cargado directamente")
            return df

        except Exception:
            # Not a direct .dta, try as ZIP
            if self.verbose:
                print(f"       [INFO] No es .dta directo, intentando como ZIP...")

        # Reset and try as ZIP
        file_data.seek(0)
        try:
            with zipfile.ZipFile(file_data) as zf:
                # List all files in archive
                all_files = zf.namelist()

                if self.verbose:
                    print(f"       [DEBUG] Archivos en ZIP: {all_files[:5]}...")

                # Search for .dta files (case-insensitive)
                dta_files = [name for name in all_files if name.lower().endswith('.dta')]

                if not dta_files:
                    # Try alternate extensions or nested structures
                    if self.verbose:
                        print(f"       [WARNING] No se encontraron archivos .dta")
                        print(f"       [INFO] Archivos disponibles: {all_files}")
                    return None

                # Use first .dta found (usually the main survey file)
                dta_filename = dta_files[0]

                if self.verbose:
                    print(f"       [INFO] Extrayendo: {dta_filename}")

                # Extract to memory and load
                with zf.open(dta_filename) as dta_file:
                    dta_buffer = io.BytesIO(dta_file.read())

                    # Load with pandas (supports Stata 13+)
                    if self.verbose:
                        print(f"       [INFO] Cargando con pandas...")

                    # Read Stata file with proper encoding handling
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        df = pd.read_stata(dta_buffer, convert_categoricals=False)

                    return df

        except zipfile.BadZipFile:
            if self.verbose:
                print(f"       [ERROR] Archivo no es un ZIP válido ni .dta directo")
            # Could be RAR (requires rarfile library)
            return None
        except Exception as e:
            if self.verbose:
                # Avoid encoding errors when printing exception
                try:
                    print(f"       [ERROR] Error al extraer: {str(e)[:100]}")
                except:
                    print(f"       [ERROR] Error al extraer (encoding issue)")
            return None
