*! usecasen: Descarga Inteligente de la Encuesta CASEN (Chile)
*! Version 3.2.0: 2026/02/24
*! Author: Generado con Claude Code
*! Pontificia Universidad Católica de Chile

capture program drop usecasen
program define usecasen
    version 14.0
    set more off

    syntax , Years(numlist) [path(string) replace clear preserve noraw ///
        strict retries(integer 2) timeout(integer 120) debugscore]

    if `retries' < 1 {
        di as error "retries() debe ser >= 1"
        exit 198
    }
    if `timeout' < 0 {
        di as error "timeout() debe ser >= 0"
        exit 198
    }
    loc strict_mode 0
    if "`strict'" != "" loc strict_mode 1

    * --- 1. PREPARACIÓN ---
    loc has_init_data 0
    if "`preserve'" != "" {
        tempfile _init_data
        cap save `_init_data', replace
        if _rc == 0 {
            loc has_init_data 1
        }
        else {
            di as text "[WARNING] preserve solicitado pero no hay datos activos para restaurar."
        }
    }

    if "`path'" == "" loc path "."

    * Detectar OS
    loc os_type "`c(os)'"
    loc is_windows = 0
    if "`os_type'" == "Windows" {
        loc is_windows = 1
    }

    * Crear estructura de carpetas
    foreach d in "`path'" "`path'/casen_raw" "`path'/casen_dta" "`path'/casen_temp" {
        cap mkdir "`d'"
    }
    loc raw_dir "`path'/casen_raw"
    loc dta_dir "`path'/casen_dta"
    loc temp_dir "`path'/casen_temp"

    * --- 2. DETECTAR DESCOMPRESORES ---
    loc zip_extractor ""
    loc zip_extractor_type ""
    loc rar_extractor ""
    loc rar_extractor_type ""

    if `is_windows' == 1 {
        * ZIP: fallback nativo de Windows
        loc zip_extractor_type "powershell"

        * unRAR/WinRAR para RAR (priorizar unRAR.exe CLI)
        loc winrar_paths `""C:/Program Files/WinRAR/unRAR.exe" "C:/Program Files (x86)/WinRAR/unRAR.exe" "C:/Program Files/WinRAR/WinRAR.exe" "C:/Program Files (x86)/WinRAR/WinRAR.exe" "D:/Program Files/WinRAR/WinRAR.exe" "C:/WinRAR/WinRAR.exe""'
        foreach p of loc winrar_paths {
            cap confirm file "`p'"
            if _rc == 0 {
                loc rar_extractor "`p'"
                loc rar_extractor_type "winrar"
                continue, break
            }
        }

        * 7-Zip para ZIP/RAR
        loc sevenzip_paths `""C:/Program Files/7-Zip/7z.exe" "C:/Program Files (x86)/7-Zip/7z.exe" "D:/Program Files/7-Zip/7z.exe""'
        foreach p of loc sevenzip_paths {
            cap confirm file "`p'"
            if _rc == 0 {
                loc zip_extractor "`p'"
                loc zip_extractor_type "7zip_win"
                if "`rar_extractor'" == "" {
                    loc rar_extractor "`p'"
                    loc rar_extractor_type "7zip_win"
                }
                continue, break
            }
        }
    }
    else {
        * Unix/macOS: extractor ZIP
        foreach prog in unzip 7z unar bsdtar {
            cap shell which `prog' > /dev/null 2>&1
            if _rc == 0 {
                loc zip_extractor "`prog'"
                loc zip_extractor_type "`prog'"
                if "`prog'" == "7z" loc zip_extractor_type "7zip_unix"
                continue, break
            }
        }

        * Unix/macOS: extractor RAR
        foreach prog in unrar 7z unar bsdtar {
            cap shell which `prog' > /dev/null 2>&1
            if _rc == 0 {
                loc rar_extractor "`prog'"
                loc rar_extractor_type "`prog'"
                if "`prog'" == "7z" loc rar_extractor_type "7zip_unix"
                continue, break
            }
        }
    }

    if "`zip_extractor_type'" == "" {
        di as text "[WARNING] No se detectó extractor ZIP."
        di as text "[INFO] Se podrán procesar solo enlaces .dta directos."
    }

    if "`rar_extractor_type'" == "" {
        di as text "[WARNING] No se detectó extractor RAR."
        di as text "[INFO] Si un año publica .rar, ese año fallará."
    }

    * --- 3. HEADER ---
    di as text ""
    di as result "{hline 60}"
    di as result "  USECASEN v3.2.0 - Encuesta CASEN"
    di as result "{hline 60}"
    di as text "Años: " as result "`years'" as text " | Ruta: " as result "`path'"
    if "`debugscore'" != "" {
        di as text "Modo debugscore=ON | retries=`retries' | timeout=`timeout' | strict=`strict_mode'"
    }
    di as text ""

    loc base_domain "https://observatorio.ministeriodesarrollosocial.gob.cl"
    loc last_dta ""
    loc had_error 0
    loc first_error_year ""

    * --- 4. BUCLE PRINCIPAL ---
    foreach y of loc years {
        loc final_dta "`dta_dir'/casen_`y'.dta"

        * Cache Check
        cap confirm file "`final_dta'"
        if _rc == 0 & "`replace'" == "" {
            di as text "  `y': " as result "[CACHE]" as text " Usando archivo local"
            loc last_dta "`final_dta'"
            continue
        }

        di as text "  `y': Buscando enlace..."

        * A. Descarga del HTML
        loc page_url "`base_domain'/encuesta-casen-`y'"
        tempfile html_file
        loc page_rc 1
        forvalues page_try = 1/`retries' {
            if `timeout' > 0 {
                cap copy "`page_url'" "`html_file'", replace timeout(`timeout')
                loc page_rc = _rc
                if `page_rc' == 198 {
                    cap copy "`page_url'" "`html_file'", replace
                    loc page_rc = _rc
                }
            }
            else {
                cap copy "`page_url'" "`html_file'", replace
                loc page_rc = _rc
            }

            if `page_rc' == 0 continue, break
            if "`debugscore'" != "" {
            di as text "  `y': [DEBUG] intento web `page_try' falló (rc=`page_rc')"
            }
            if `page_try' < `retries' sleep 1000
        }

        if `page_rc' != 0 {
            di as error "  `y': " as error "[ERROR]" as text " No se pudo acceder a la web"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        * B. PARSING INTELIGENTE (MATA con Scoring)
        mata: _parse_casen_smart("`html_file'", "`y'")
        loc found_url "${CASEN_FOUND_URL}"
        global CASEN_FOUND_URL ""

        if "`found_url'" == "" {
            di as error "  `y': " as error "[ERROR]" as text " No se encontró archivo de datos"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        * C. Normalizar URL
        if strpos("`found_url'", "http") != 1 {
            if strpos("`found_url'", "/") == 1 {
                loc found_url "`base_domain'`found_url'"
            }
            else {
                loc found_url "`base_domain'/`found_url'"
            }
        }

        * Detectar extensión (soporta .dta directo, .zip y .rar)
        if strpos(lower("`found_url'"), ".dta") > 0 & strpos(lower("`found_url'"), ".zip") == 0 ///
            & strpos(lower("`found_url'"), ".rar") == 0 {
            loc found_ext "dta"
        }
        else if strpos(lower("`found_url'"), ".zip") > 0 {
            loc found_ext "zip"
        }
        else if strpos(lower("`found_url'"), ".rar") > 0 {
            loc found_ext "rar"
        }
        else {
            di as error "  `y': " as error "[ERROR]" as text " Extensión no soportada en URL"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        if "`debugscore'" != "" {
            di as text "  `y': [DEBUG] URL elegida: `found_url'"
            di as text "  `y': [DEBUG] Extensión detectada: `found_ext'"
        }

        * D. Descarga
        loc outfile "`raw_dir'/casen_`y'.`found_ext'"
        loc download_url = subinstr("`found_url'", " ", "%20", .)
        loc file_rc 1
        forvalues file_try = 1/`retries' {
            if `timeout' > 0 {
                cap copy "`download_url'" "`outfile'", replace timeout(`timeout')
                loc file_rc = _rc
                if `file_rc' == 198 {
                    cap copy "`download_url'" "`outfile'", replace
                    loc file_rc = _rc
                }
            }
            else {
                cap copy "`download_url'" "`outfile'", replace
                loc file_rc = _rc
            }

            if `file_rc' == 0 continue, break
            if "`debugscore'" != "" {
                di as text "  `y': [DEBUG] intento descarga `file_try' falló (rc=`file_rc')"
            }
            if `file_try' < `retries' sleep 1000
        }

        if `file_rc' != 0 {
            di as error "  `y': " as error "[ERROR]" as text " Falló la descarga"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        * E. Resolver archivo .dta final
        loc found_dta ""
        loc unzip_dir ""
        loc extraction_rc 0

        if "`found_ext'" == "dta" {
            loc found_dta "`outfile'"
        }
        else {
            if "`found_ext'" == "zip" & "`zip_extractor_type'" == "" {
                di as error "  `y': " as error "[ERROR]" as text " No hay extractor ZIP disponible"
                loc had_error 1
                if "`first_error_year'" == "" loc first_error_year "`y'"
                if `strict_mode' == 1 {
                    di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                    continue, break
                }
                continue
            }

            if "`found_ext'" == "rar" & "`rar_extractor_type'" == "" {
                di as error "  `y': " as error "[ERROR]" as text " Se requiere WinRAR/7-Zip/unrar/unar/bsdtar para extraer RAR"
                loc had_error 1
                if "`first_error_year'" == "" loc first_error_year "`y'"
                if `strict_mode' == 1 {
                    di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                    continue, break
                }
                continue
            }

            loc unzip_dir "`temp_dir'/casen_`y'_extract"
            cap mkdir "`unzip_dir'"

            if "`found_ext'" == "zip" {
                if `is_windows' == 1 {
                    if "`zip_extractor_type'" == "7zip_win" {
                        loc sevenzip_cmd = subinstr("`zip_extractor'", "/", "\", .)
                        qui shell "`sevenzip_cmd'" x -y -o"`unzip_dir'" "`outfile'" >nul 2>&1
                        loc extraction_rc = _rc
                    }
                    else {
                        qui shell powershell -NoProfile -Command "Expand-Archive -Path '`outfile'' -DestinationPath '`unzip_dir'' -Force" >nul 2>&1
                        loc extraction_rc = _rc
                    }
                }
                else if "`zip_extractor_type'" == "unzip" {
                    qui shell unzip -o -q "`outfile'" -d "`unzip_dir'" 2>/dev/null
                    loc extraction_rc = _rc
                }
                else if "`zip_extractor_type'" == "7zip_unix" {
                    qui shell 7z x -y -o"`unzip_dir'" "`outfile'" >/dev/null 2>&1
                    loc extraction_rc = _rc
                }
                else if "`zip_extractor_type'" == "unar" {
                    qui shell unar -q -o "`unzip_dir'" "`outfile'" 2>/dev/null
                    loc extraction_rc = _rc
                }
                else if "`zip_extractor_type'" == "bsdtar" {
                    qui shell bsdtar -xf "`outfile'" -C "`unzip_dir'" >/dev/null 2>&1
                    loc extraction_rc = _rc
                }
                else {
                    di as error "  `y': " as error "[ERROR]" as text " Extractor ZIP no soportado: `zip_extractor_type'"
                    loc had_error 1
                    if "`first_error_year'" == "" loc first_error_year "`y'"
                    if `strict_mode' == 1 {
                        di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                        continue, break
                    }
                    continue
                }
            }
            else if "`found_ext'" == "rar" {
                if "`rar_extractor_type'" == "winrar" {
                    loc winrar_cmd = subinstr("`rar_extractor'", "/", "\", .)
                    qui shell "`winrar_cmd'" x -o+ -inul "`outfile'" "`unzip_dir'\" >nul 2>&1
                    loc extraction_rc = _rc
                }
                else if "`rar_extractor_type'" == "7zip_win" {
                    loc sevenzip_cmd = subinstr("`rar_extractor'", "/", "\", .)
                    qui shell "`sevenzip_cmd'" x -y -o"`unzip_dir'" "`outfile'" >nul 2>&1
                    loc extraction_rc = _rc
                }
                else if "`rar_extractor_type'" == "unrar" {
                    qui shell unrar x -o+ -inul "`outfile'" "`unzip_dir'/" 2>/dev/null
                    loc extraction_rc = _rc
                }
                else if "`rar_extractor_type'" == "7zip_unix" {
                    qui shell 7z x -y -o"`unzip_dir'" "`outfile'" >/dev/null 2>&1
                    loc extraction_rc = _rc
                }
                else if "`rar_extractor_type'" == "unar" {
                    qui shell unar -q -o "`unzip_dir'" "`outfile'" 2>/dev/null
                    loc extraction_rc = _rc
                }
                else if "`rar_extractor_type'" == "bsdtar" {
                    qui shell bsdtar -xf "`outfile'" -C "`unzip_dir'" >/dev/null 2>&1
                    loc extraction_rc = _rc
                }
                else {
                    di as error "  `y': " as error "[ERROR]" as text " No hay extractor compatible para RAR"
                    loc had_error 1
                    if "`first_error_year'" == "" loc first_error_year "`y'"
                    if `strict_mode' == 1 {
                        di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                        continue, break
                    }
                    continue
                }
            }

            if `extraction_rc' != 0 {
                di as error "  `y': " as error "[ERROR]" as text " Falló la extracción (`found_ext')"
                loc had_error 1
                if "`first_error_year'" == "" loc first_error_year "`y'"
                if `strict_mode' == 1 {
                    di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                    continue, break
                }
                continue
            }

            sleep 500

            * F. Búsqueda recursiva de .dta + scoring
            tempfile dta_candidates
            loc list_rc 0

            if `is_windows' == 1 {
                loc unzip_dir_win = subinstr("`unzip_dir'", "/", "\", .)
                qui shell dir /s /b "`unzip_dir_win'\*.dta" > "`dta_candidates'" 2>nul
                loc list_rc = _rc
            }
            else {
                qui shell find "`unzip_dir'" -type f \( -iname "*.dta" \) > "`dta_candidates'" 2>/dev/null
                loc list_rc = _rc
            }

            if `list_rc' != 0 {
                di as error "  `y': " as error "[ERROR]" as text " No se pudo listar archivos .dta extraidos"
                loc had_error 1
                if "`first_error_year'" == "" loc first_error_year "`y'"
                if `strict_mode' == 1 {
                    di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                    continue, break
                }
                continue
            }

            loc best_score = -9999
            loc best_len = 999999
            capture file close __uc_dtalist
            cap file open __uc_dtalist using "`dta_candidates'", read text
            if _rc != 0 {
                di as error "  `y': " as error "[ERROR]" as text " No se pudo leer listado de .dta extraidos"
                loc had_error 1
                if "`first_error_year'" == "" loc first_error_year "`y'"
                if `strict_mode' == 1 {
                    di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                    continue, break
                }
                continue
            }

            file read __uc_dtalist dta_line
            while r(eof) == 0 {
                loc candidate = trim("`dta_line'")
                if "`candidate'" != "" {
                    loc candidate = subinstr("`candidate'", "\", "/", .)
                    if strpos("`candidate'", ":") == 0 & substr("`candidate'", 1, 1) != "/" {
                        if substr("`candidate'", 1, 2) == "./" {
                            loc candidate = substr("`candidate'", 3, .)
                        }
                        loc candidate "`unzip_dir'/`candidate'"
                    }

                    loc cand_l = lower("`candidate'")
                    loc score = 0

                    * Premios
                    if strpos("`cand_l'", "casen") > 0 loc score = `score' + 30
                    if strpos("`cand_l'", "stata") > 0 loc score = `score' + 100
                    if strpos("`cand_l'", ".dta") > 0 loc score = `score' + 80
                    if strpos("`cand_l'", "`y'") > 0 loc score = `score' + 50
                    if strpos("`cand_l'", "storage/docs/casen") > 0 loc score = `score' + 40
                    if strpos("`cand_l'", "casen_`y'") > 0 loc score = `score' + 35
                    if strpos("`cand_l'", "personas") > 0 loc score = `score' + 10
                    if strpos("`cand_l'", "hogares") > 0 loc score = `score' + 5
                    if regexm("`cand_l'", "(^|/)casen[_-]*`y'([_-]*stata)?\.dta$") loc score = `score' + 120
                    if regexm("`cand_l'", "(^|/)casen[_-].*\.dta$") loc score = `score' + 15

                    * Penalizaciones
                    if strpos("`cand_l'", "spss") > 0 loc score = `score' - 100
                    if strpos("`cand_l'", ".sav") > 0 loc score = `score' - 100
                    if strpos("`cand_l'", "sas") > 0 loc score = `score' - 80
                    if strpos("`cand_l'", "csv") > 0 loc score = `score' - 50
                    if strpos("`cand_l'", "manual") > 0 loc score = `score' - 60
                    if strpos("`cand_l'", "libro") > 0 loc score = `score' - 60
                    if strpos("`cand_l'", "metodologia") > 0 loc score = `score' - 60
                    if strpos("`cand_l'", "codigos") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "cuestionario") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "factor") > 0 loc score = `score' - 50
                    if strpos("`cand_l'", "raking") > 0 loc score = `score' - 50
                    if strpos("`cand_l'", "quintil") > 0 loc score = `score' - 30
                    if strpos("`cand_l'", "decil") > 0 loc score = `score' - 30
                    if strpos("`cand_l'", "ingresos") > 0 loc score = `score' - 25
                    if strpos("`cand_l'", "metadato") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "diccionario") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "expansion") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "ponder") > 0 loc score = `score' - 40
                    if strpos("`cand_l'", "deflactor") > 0 loc score = `score' - 35
                    if strpos("`cand_l'", "linea_pobreza") > 0 loc score = `score' - 35

                    if "`debugscore'" != "" {
                        di as text "    [DEBUG][`y'] score=`score' | `candidate'"
                    }

                    loc cand_len = length("`cand_l'")

                    * Desempate: para mismo score prioriza nombre de archivo más corto
                    if `score' > `best_score' | (`score' == `best_score' & `cand_len' < `best_len') {
                        loc best_score = `score'
                        loc best_len = `cand_len'
                        loc found_dta "`candidate'"
                    }
                }
                file read __uc_dtalist dta_line
            }
            file close __uc_dtalist
        }

        if "`debugscore'" != "" & "`found_dta'" != "" {
            di as text "  `y': [DEBUG] .dta seleccionado: `found_dta' (score=`best_score')"
        }

        if "`found_dta'" == "" {
            di as error "  `y': " as error "[ERROR]" as text " No se encontró .dta dentro del archivo"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        * G. Copiar con nombre estandarizado
        cap copy "`found_dta'" "`final_dta'", replace
        if _rc != 0 {
            di as error "  `y': " as error "[ERROR]" as text " No se pudo copiar .dta final"
            loc had_error 1
            if "`first_error_year'" == "" loc first_error_year "`y'"
            if `strict_mode' == 1 {
                di as error "  `y': " as error "[STRICT]" as text " Abortando en primer error"
                continue, break
            }
            continue
        }

        * H. Unicode para años antiguos
        if `y' < 2013 {
            clear
            cap unicode analyze "`final_dta'"
            cap unicode encoding set "latin1"
            cap unicode translate "`final_dta'"
        }

        * I. Comprimir
        preserve
        qui use "`final_dta'", clear
        qui compress
        qui save "`final_dta'", replace
        restore

        loc last_dta "`final_dta'"

        * J. Limpieza de extracción temporal
        if "`unzip_dir'" != "" {
            if `is_windows' == 1 {
                cap shell rd /s /q "`unzip_dir'"
            }
            else {
                cap shell rm -rf "`unzip_dir'" 2>/dev/null
            }
        }

        di as result "  `y': " as result "[OK]" as text " casen_`y'.dta"
    }

    * --- 5. LIMPIEZA FINAL ---
    * Borrar carpeta temp
    if `is_windows' == 1 {
        cap shell rd /s /q "`temp_dir'"
    }
    else {
        cap shell rm -rf "`temp_dir'" 2>/dev/null
    }

    * Borrar carpeta raw si se especificó noraw
    if "`noraw'" != "" {
        if `is_windows' == 1 {
            cap shell rd /s /q "`raw_dir'"
        }
        else {
            cap shell rm -rf "`raw_dir'" 2>/dev/null
        }
    }

    di as text ""
    di as result "{hline 60}"

    if `strict_mode' == 1 & `had_error' == 1 {
        if "`preserve'" != "" & `has_init_data' == 1 {
            cap use `_init_data', clear
        }
        di as error "usecasen finalizó en modo strict tras falla en año `first_error_year'"
        exit 459
    }

    * --- 6. CARGAR SI SE PIDIÓ ---
    if "`clear'" != "" & "`last_dta'" != "" {
        qui use "`last_dta'", clear
        di as text "Base cargada: " as result "`last_dta'"
    }
    else if "`preserve'" != "" & "`clear'" == "" & `has_init_data' == 1 {
        cap use `_init_data', clear
    }

end

********************************************************************************
* MOTOR MATA: PARSER INTELIGENTE CON SCORING
********************************************************************************

capture mata: mata drop _parse_casen_smart()
mata:
void _parse_casen_smart(string scalar html_file, string scalar year)
{
    string scalar content_raw, content_lc, line, best_url, candidate, candidate_lc, c, year_lc, last_char
    real scalar fh, j, content_len, score, max_score
    string rowvector extensions
    real scalar ext_i, pos_search, ext_pos, abs_ext_pos, start_pos

    // 1. Leer archivo
    fh = fopen(html_file, "r")
    if (fh < 0) {
        st_global("CASEN_FOUND_URL", "")
        return
    }

    content_raw = ""
    while ((line = fget(fh)) != J(0,0,"")) {
        content_raw = content_raw + line
    }
    fclose(fh)

    // 2. Normalizar para scoring sin perder mayúsculas originales
    content_lc = strlower(content_raw)
    content_len = strlen(content_lc)
    year_lc = strlower(year)

    best_url = ""
    max_score = -9999

    // 3. Buscar extensiones .dta, .zip y .rar
    extensions = (".dta", ".zip", ".rar")

    for (ext_i = 1; ext_i <= cols(extensions); ext_i++) {
        pos_search = 1

        while (pos_search < content_len) {
            // Buscar extensión
            ext_pos = strpos(substr(content_lc, pos_search, .), extensions[ext_i])
            if (ext_pos == 0) break

            abs_ext_pos = pos_search + ext_pos - 1

            // 4. Back-tracing: encontrar inicio de URL
            candidate = ""
            for (j = abs_ext_pos; j >= max((1, abs_ext_pos - 300)); j--) {
                c = substr(content_lc, j, 1)
                if (c == char(34) | c == "'" | c == "=" | c == ">" | c == " " | c == "(") {
                    start_pos = j + 1
                    candidate = substr(content_raw, start_pos, (abs_ext_pos + strlen(extensions[ext_i]) - 1) - start_pos + 1)
                    break
                }
            }

            // 5. Validar y puntuar
            if (strlen(candidate) > 10) {
                candidate_lc = strlower(candidate)

                // 6. SCORING SYSTEM
                score = 0

                // Premios
                if (strpos(candidate_lc, "casen") > 0) score = score + 30
                if (strpos(candidate_lc, "stata") > 0) score = score + 100
                if (strpos(candidate_lc, ".dta") > 0) score = score + 80
                if (strpos(candidate_lc, year_lc) > 0) score = score + 50
                if (strpos(candidate_lc, "storage/docs/casen") > 0) score = score + 40
                if (strpos(candidate_lc, "casen_") > 0) score = score + 15
                if (strpos(candidate_lc, "casen" + year_lc) > 0) score = score + 20
                if (strpos(candidate_lc, "personas") > 0) score = score + 10
                if (strpos(candidate_lc, "hogares") > 0) score = score + 5

                // Penalizaciones
                if (strpos(candidate_lc, "spss") > 0) score = score - 100
                if (strpos(candidate_lc, ".sav") > 0) score = score - 100
                if (strpos(candidate_lc, "sas") > 0) score = score - 80
                if (strpos(candidate_lc, "csv") > 0) score = score - 50
                if (strpos(candidate_lc, "manual") > 0) score = score - 60
                if (strpos(candidate_lc, "libro") > 0) score = score - 60
                if (strpos(candidate_lc, "metodologia") > 0) score = score - 60
                if (strpos(candidate_lc, "codigos") > 0) score = score - 40
                if (strpos(candidate_lc, "cuestionario") > 0) score = score - 40
                if (strpos(candidate_lc, "factor") > 0) score = score - 50
                if (strpos(candidate_lc, "raking") > 0) score = score - 50
                if (strpos(candidate_lc, "quintil") > 0) score = score - 30
                if (strpos(candidate_lc, "decil") > 0) score = score - 30
                if (strpos(candidate_lc, "ingresos") > 0) score = score - 25
                if (strpos(candidate_lc, "expansion") > 0) score = score - 40
                if (strpos(candidate_lc, "ponder") > 0) score = score - 40
                if (strpos(candidate_lc, "deflactor") > 0) score = score - 35
                if (strpos(candidate_lc, "linea_pobreza") > 0) score = score - 35

                // Actualizar ganador
                if (score > max_score) {
                    max_score = score
                    best_url = candidate
                }
            }

            pos_search = abs_ext_pos + 1
        }
    }

    // Limpiar URL
    if (best_url != "") {
        best_url = strtrim(best_url)

        if (strlen(best_url) > 0) {
            if (substr(best_url, 1, 1) == char(34) | substr(best_url, 1, 1) == "'") {
                best_url = substr(best_url, 2, .)
            }
        }

        if (strlen(best_url) > 0) {
            last_char = substr(best_url, strlen(best_url), 1)
            if (last_char == char(34) | last_char == "'" | last_char == ")") {
                best_url = substr(best_url, 1, strlen(best_url) - 1)
            }
        }
    }

    st_global("CASEN_FOUND_URL", best_url)
}
end

********************************************************************************
* FIN DEL ARCHIVO
********************************************************************************
