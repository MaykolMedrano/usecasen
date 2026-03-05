"""
CASEN Downloader Module

Handles web scraping, file download, and data extraction for CASEN surveys.
Implements intelligent URL scoring system ported from MATA.
"""

import io
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Optional, Dict, List
import warnings

import pandas as pd
import requests
from bs4 import BeautifulSoup
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

    # Common Windows install locations for WinRAR.
    WINRAR_PATHS = [
        r"C:\Program Files\WinRAR\WinRAR.exe",
        r"C:\Program Files (x86)\WinRAR\WinRAR.exe",
        r"D:\Program Files\WinRAR\WinRAR.exe",
    ]

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

    def _log(self, message: str) -> None:
        """
        Print message only when verbose mode is enabled.
        """
        if self.verbose:
            print(message)

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

    def _fetch_best_url(self, year: int) -> Optional[str]:
        """
        Resolve the best candidate download URL for a given CASEN year.
        """
        page_url = self._get_year_url(year)
        html_content = self._fetch_html(page_url)

        if html_content is None:
            return None

        best_url = self._get_best_link(html_content, str(year))
        if not best_url:
            return None

        return self._normalize_url(best_url)

    def download_casen(self, year: int) -> Optional[pd.DataFrame]:
        """
        Download and parse CASEN survey for a given year.

        Args:
            year: Survey year (e.g., 2017, 2020, 2022)

        Returns:
            DataFrame with survey data, or None if failed
        """
        self._log(f"  {year}: Buscando enlace...")

        best_url = self._fetch_best_url(year)

        if not best_url:
            self._log(f"  {year}: [ERROR] No se encontró archivo de datos")
            return None

        # Step 4: Download file (in-memory)
        self._log(f"  {year}: Descargando...")
        file_data = self._download_file(best_url)

        if file_data is None:
            self._log(f"  {year}: [ERROR] Falló la descarga")
            return None

        # Step 5: Extract and load .dta (in-memory)
        self._log(f"  {year}: Procesando archivo...")
        df = self._extract_and_load_dta(file_data, year)

        if df is None:
            self._log(f"  {year}: [ERROR] No se encontró .dta en el archivo")
            return None

        self._log(f"  {year}: [OK] {len(df):,} observaciones cargadas")
        return df

    def download_multiple(self, years: List[int], load_to_stata: bool = False) -> Dict[int, pd.DataFrame]:
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

        self._log("=" * 60)
        self._log("  USECASEN Python v1.0 - Encuesta CASEN")
        self._log("=" * 60)
        self._log(f"Años: {years}")

        # Test connectivity
        if self.verbose:
            print("\nProbando conectividad al servidor...")
            if self.test_connectivity():
                print("  [OK] Servidor accesible")
            else:
                print("  [WARNING] Servidor no responde - las descargas pueden fallar")
                print("  [SUGERENCIA] Verifique su conexión a Internet")

        self._log("")

        for year in years:
            df = self.download_casen(year)
            if df is not None:
                results[year] = df
                last_df = df

        self._log("=" * 60)

        # Load to Stata if requested (requires stata_io module)
        if load_to_stata and last_df is not None:
            from casen.stata_io import to_stata
            self._log("Cargando última base a Stata...")
            to_stata(last_df)

        return results

    def _is_rar_file(self, file_data: io.BytesIO) -> bool:
        """
        Detect if payload is a RAR archive by signature.
        """
        file_data.seek(0)
        signature = file_data.read(8)
        file_data.seek(0)

        # RAR4: 52 61 72 21 1A 07 00
        # RAR5: 52 61 72 21 1A 07 01 00
        return signature.startswith(b"Rar!\x1a\x07\x00") or signature.startswith(b"Rar!\x1a\x07\x01\x00")

    def _select_best_dta_candidate(self, candidates: List[tuple], year: int) -> Optional[str]:
        """
        Pick the best .dta candidate from (name, size) entries.
        """
        if not candidates:
            return None

        year_str = str(year)
        scored = []
        for name, size in candidates:
            score = self._calculate_score(name, year_str)
            scored.append((score, size, name))

        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return scored[0][2]

    def _extract_dta_from_zip(self, file_data: io.BytesIO, year: int) -> Optional[io.BytesIO]:
        """
        Extract best .dta file from ZIP archive.
        """
        file_data.seek(0)
        with zipfile.ZipFile(file_data) as zf:
            candidates = []
            for info in zf.infolist():
                name = info.filename
                lower_name = name.lower()
                if lower_name.endswith(".dta") and not lower_name.startswith("__macosx/"):
                    candidates.append((name, info.file_size))

            if not candidates:
                return None

            best_name = self._select_best_dta_candidate(candidates, year)
            if best_name is None:
                return None

            if self.verbose:
                print(f"       [INFO] Extrayendo desde ZIP: {best_name}")

            with zf.open(best_name) as dta_file:
                return io.BytesIO(dta_file.read())

    def _build_rar_extract_commands(self, rar_path: str, output_dir: str) -> List[List[str]]:
        """
        Build candidate commands to extract RAR archives across platforms.
        """
        commands: List[List[str]] = []
        output_with_sep = output_dir + os.sep

        # Common CLI tools from PATH.
        if shutil.which("unrar"):
            commands.append(["unrar", "x", "-o+", "-inul", rar_path, output_with_sep])
        if shutil.which("7z"):
            commands.append(["7z", "x", "-y", f"-o{output_dir}", rar_path])
        if shutil.which("unar"):
            commands.append(["unar", "-q", "-o", output_dir, rar_path])
        if shutil.which("bsdtar"):
            commands.append(["bsdtar", "-xf", rar_path, "-C", output_dir])

        # Explicit WinRAR path fallback on Windows.
        for winrar_path in self.WINRAR_PATHS:
            if Path(winrar_path).exists():
                commands.append([winrar_path, "x", "-o+", "-inul", rar_path, output_with_sep])

        return commands

    def _extract_dta_from_rar(self, file_data: io.BytesIO, year: int) -> Optional[io.BytesIO]:
        """
        Extract best .dta file from RAR archive using external tools.
        """
        with tempfile.TemporaryDirectory(prefix="casen_rar_") as temp_dir:
            temp_path = Path(temp_dir)
            rar_path = temp_path / "casen_payload.rar"
            out_dir = temp_path / "extract"
            out_dir.mkdir(parents=True, exist_ok=True)

            file_data.seek(0)
            rar_path.write_bytes(file_data.read())

            commands = self._build_rar_extract_commands(str(rar_path), str(out_dir))
            if not commands:
                if self.verbose:
                    print("       [ERROR] No hay extractor RAR disponible (WinRAR/7z/unrar/unar/bsdtar)")
                return None

            extracted = False
            for cmd in commands:
                try:
                    result = subprocess.run(
                        cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=max(120, self.timeout * 8),
                        check=False,
                    )
                    if result.returncode == 0:
                        extracted = True
                        break
                except Exception:
                    continue

            if not extracted:
                if self.verbose:
                    print("       [ERROR] No se pudo descomprimir RAR con las herramientas disponibles")
                return None

            dta_paths = [path for path in out_dir.rglob("*.dta") if path.is_file()]
            if not dta_paths:
                if self.verbose:
                    print("       [ERROR] El RAR no contiene archivos .dta")
                return None

            candidates = [
                (str(path.relative_to(out_dir)).replace("\\", "/"), path.stat().st_size)
                for path in dta_paths
            ]
            best_rel = self._select_best_dta_candidate(candidates, year)
            if best_rel is None:
                return None

            best_path = out_dir / Path(best_rel)
            if self.verbose:
                print(f"       [INFO] Extrayendo desde RAR: {best_rel}")

            return io.BytesIO(best_path.read_bytes())

    def extract_dta_buffer(self, file_data: io.BytesIO, year: int) -> Optional[io.BytesIO]:
        """
        Resolve payload into a .dta buffer (supports direct .dta, ZIP, and RAR).
        """
        file_data.seek(0)

        if zipfile.is_zipfile(file_data):
            if self.verbose:
                print("       [INFO] Archivo ZIP detectado")
            return self._extract_dta_from_zip(file_data, year)

        if self._is_rar_file(file_data):
            if self.verbose:
                print("       [INFO] Archivo RAR detectado")
            return self._extract_dta_from_rar(file_data, year)

        # Assume direct .dta payload.
        if self.verbose:
            print("       [INFO] Asumiendo archivo .dta directo")
        file_data.seek(0)
        return file_data

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
        # Keep original content for href extraction; use lowercase only for fallback parsing.
        content = html_content.lower()

        best_url = None
        max_score = -9999

        # Extensions to search for (prioritize .dta direct, then compressed)
        extension_patterns = [
            '.dta',
            '.dta.zip',
            '.sav.zip',
            '.dta.rar',
            '.sav.rar',
            '.zip',
            '.rar',
        ]

        # First pass: parse real href attributes to preserve exact casing/spaces.
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            href_candidates = []

            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href'].strip()
                href_lower = href.lower()

                if any(ext in href_lower for ext in extension_patterns):
                    href_candidates.append(href)

            for candidate in href_candidates:
                score = self._calculate_score(candidate, year)
                if score > max_score:
                    max_score = score
                    best_url = candidate

            if best_url is not None:
                return best_url

        except Exception:
            # Fallback to legacy text parser if HTML parsing fails.
            pass

        # Fallback pass: legacy text scanning.
        for ext in extension_patterns:
            pos = 0
            while True:
                ext_pos = content.find(ext, pos)
                if ext_pos == -1:
                    break

                candidate = None
                search_start = max(0, ext_pos - 500)

                storage_pos = content.rfind('storage/', search_start, ext_pos)
                if storage_pos != -1:
                    end_pos = ext_pos + len(ext)
                    candidate = content[storage_pos:end_pos]
                else:
                    start_delimiters = ['"', "'", '=', '>', '(', '\n', '\r']

                    for j in range(ext_pos, search_start, -1):
                        if content[j] in start_delimiters:
                            start_pos = j + 1
                            end_pos = ext_pos + len(ext)
                            candidate = content[start_pos:end_pos]
                            break

                if candidate and len(candidate) > 10:
                    candidate = candidate.strip()

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
        url = url.lower()
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
        # Strongly prefer canonical full-survey filenames when present.
        if f'casen_{year}.dta' in url:
            score += 180
        if f'casen_{year}_stata' in url:
            score += 160
        if f'casen{year}stata' in url:
            score += 240
        if f'casen{year}' in url and 'principal' in url:
            score += 220
        if f'casen{year}' in url and 'full' in url and 'h4_' not in url:
            score += 180

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
        # Penalize known auxiliary files to avoid non-main datasets.
        if 'factor' in url:
            score -= 160
        if 'raking' in url:
            score -= 140
        if 'deciles' in url:
            score -= 120
        if 'quintil' in url:
            score -= 120
        if 'complementaria' in url:
            score -= 180
        if 'provincia_comuna' in url:
            score -= 80
        if '/est_' in url:
            score -= 220
        if 'ingresos_originales' in url:
            score -= 240
        if 'ingresosoriginal' in url:
            score -= 220
        if 'ingresos_ajustados' in url or 'ingreso_ajustados' in url or 'ingresosajustados' in url:
            score -= 220
        if 'ingresos_mt' in url or 'ingresos_mn' in url or '_mt_' in url or '_mn_' in url:
            score -= 180
        if 'h4_full' in url:
            score -= 220
        if 'h4_r2' in url:
            score -= 180

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

        # Try each variant with HEAD first; fallback to GET when HEAD is blocked.
        for variant in variants:
            try:
                test_url = variant.replace(' ', '%20')
                response = self.session.head(test_url, timeout=5, allow_redirects=True)

                if response.status_code == 200:
                    if self.verbose and variant != url:
                        print(f"       [INFO] Usando variante: {variant}")
                    return variant

                if response.status_code in (403, 405):
                    response = self.session.get(test_url, timeout=10, stream=True, allow_redirects=True)
                    status_code = response.status_code
                    response.close()

                    if status_code == 200:
                        if self.verbose and variant != url:
                            print(f"       [INFO] Usando variante: {variant}")
                        return variant

            except requests.RequestException:
                try:
                    test_url = variant.replace(' ', '%20')
                    response = self.session.get(test_url, timeout=10, stream=True, allow_redirects=True)
                    status_code = response.status_code
                    response.close()

                    if status_code == 200:
                        if self.verbose:
                            if variant != url:
                                print(f"       [INFO] Usando variante: {variant}")
                        return variant
                except requests.RequestException:
                    continue
            except Exception:
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

    def _read_stata_dataframe(self, dta_buffer: io.BytesIO) -> Optional[pd.DataFrame]:
        """
        Read a .dta buffer into DataFrame with pandas, falling back to pyreadstat
        for legacy Stata versions not supported by pandas.
        """
        dta_buffer.seek(0)

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return pd.read_stata(dta_buffer, convert_categoricals=False)
        except Exception as pandas_error:
            try:
                import pyreadstat  # type: ignore
            except Exception:
                if self.verbose:
                    print(f"       [ERROR] Error al leer .dta con pandas: {str(pandas_error)[:120]}")
                    print("       [SUGERENCIA] Instale 'pyreadstat' para soportar versiones Stata antiguas")
                return None

            # pyreadstat reads from path; use a temporary file for compatibility.
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".dta", delete=False) as tmp_file:
                    dta_buffer.seek(0)
                    tmp_file.write(dta_buffer.read())
                    tmp_path = tmp_file.name

                df, _meta = pyreadstat.read_dta(
                    tmp_path,
                    apply_value_formats=False,
                    formats_as_category=False,
                )
                return df
            except Exception as pyreadstat_error:
                if self.verbose:
                    print(f"       [ERROR] Error al leer .dta con pandas: {str(pandas_error)[:120]}")
                    print(f"       [ERROR] Error al leer .dta con pyreadstat: {str(pyreadstat_error)[:120]}")
                return None
            finally:
                if tmp_path and Path(tmp_path).exists():
                    try:
                        Path(tmp_path).unlink()
                    except Exception:
                        pass

    def _extract_and_load_dta(self, file_data: io.BytesIO, year: int) -> Optional[pd.DataFrame]:
        """
        Extract .dta from ZIP/RAR (in-memory) or load directly, then convert to DataFrame.

        Args:
            file_data: BytesIO buffer with compressed file or direct .dta
            year: Survey year (for error messages)

        Returns:
            DataFrame or None
        """
        dta_buffer = self.extract_dta_buffer(file_data, year)
        if dta_buffer is None:
            if self.verbose:
                print("       [ERROR] No se pudo obtener un archivo .dta valido")
            return None

        if self.verbose:
            print("       [INFO] Cargando con pandas/pyreadstat...")

        return self._read_stata_dataframe(dta_buffer)
