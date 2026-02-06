*! usecasen: Descarga Inteligente de la Encuesta CASEN (Chile)
*! Version 3.0.0: 2024/12/19
*! Author: Generado con Claude Code
*! Pontificia Universidad Católica de Chile

capture program drop usecasen
program define usecasen
    version 14.0
    set more off

    syntax , Years(numlist) [path(string) replace clear preserve noraw]

    * --- 1. PREPARACIÓN ---
    if "`preserve'" != "" {
        tempfile _init_data
        cap save `_init_data', replace
    }

    if "`path'" == "" loc path "."
    loc current_dir "`c(pwd)'"

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

    * --- 2. DETECTAR DESCOMPRESOR ---
    loc decompressor ""
    loc decompressor_type ""

    if `is_windows' == 1 {
        * Rutas comunes de WinRAR (usando / para evitar problemas de escape)
        loc winrar_paths `""C:/Program Files/WinRAR/WinRAR.exe" "C:/Program Files (x86)/WinRAR/WinRAR.exe" "D:/Program Files/WinRAR/WinRAR.exe" "C:/WinRAR/WinRAR.exe""'

        foreach p of loc winrar_paths {
            cap confirm file "`p'"
            if _rc == 0 {
                loc decompressor "`p'"
                loc decompressor_type "winrar"
                continue, break
            }
        }

        * Si no encontró WinRAR, buscar 7-Zip
        if "`decompressor'" == "" {
            loc sevenzip_paths `""C:/Program Files/7-Zip/7z.exe" "C:/Program Files (x86)/7-Zip/7z.exe" "D:/Program Files/7-Zip/7z.exe""'

            foreach p of loc sevenzip_paths {
                cap confirm file "`p'"
                if _rc == 0 {
                    loc decompressor "`p'"
                    loc decompressor_type "7zip_win"
                    continue, break
                }
            }
        }
    }
    else {
        * Unix/macOS
        foreach prog in unrar 7z unar unzip {
            cap shell which `prog' > /dev/null 2>&1
            if _rc == 0 {
                loc decompressor "`prog'"
                loc decompressor_type "`prog'"
                if "`prog'" == "7z" loc decompressor_type "7zip_unix"
                continue, break
            }
        }
    }

    if "`decompressor'" == "" {
        di as error "ERROR: No se encontró WinRAR, 7-Zip o Unrar."
        di as error "Verifique que tenga instalado WinRAR o 7-Zip."
        di as error "OS detectado: `os_type' | is_windows=`is_windows'"
        exit 601
    }

    * --- 3. HEADER ---
    di as text ""
    di as result "{hline 60}"
    di as result "  USECASEN v3.0 - Encuesta CASEN"
    di as result "{hline 60}"
    di as text "Años: " as result "`years'" as text " | Ruta: " as result "`path'"
    di as text ""

    loc base_domain "https://observatorio.ministeriodesarrollosocial.gob.cl"
    loc last_dta ""

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
        cap copy "`page_url'" "`html_file'", replace

        if _rc != 0 {
            di as error "  `y': " as error "[ERROR]" as text " No se pudo acceder a la web"
            continue
        }

        * B. PARSING INTELIGENTE (MATA con Scoring)
        mata: _parse_casen_smart("`html_file'", "`y'")
        loc found_url "${CASEN_FOUND_URL}"
        global CASEN_FOUND_URL ""

        if "`found_url'" == "" {
            di as error "  `y': " as error "[ERROR]" as text " No se encontró archivo de datos"
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

        * Detectar extensión
        if strpos(lower("`found_url'"), ".zip") > 0 {
            loc found_ext "zip"
        }
        else {
            loc found_ext "rar"
        }

        * D. Descarga
        loc outfile "`raw_dir'/casen_`y'.`found_ext'"
        loc download_url = subinstr("`found_url'", " ", "%20", .)

        cap copy "`download_url'" "`outfile'", replace
        if _rc != 0 {
            di as error "  `y': " as error "[ERROR]" as text " Falló la descarga"
            continue
        }

        * E. Descompresión
        loc unzip_dir "`temp_dir'/casen_`y'_extract"
        cap mkdir "`unzip_dir'"

        if "`found_ext'" == "zip" {
            if `is_windows' == 1 {
                if "`decompressor_type'" == "7zip_win" {
                    loc sevenzip_cmd = subinstr("`decompressor'", "/", "\", .)
                    qui shell "`sevenzip_cmd'" x -y -o"`unzip_dir'" "`outfile'" >nul 2>&1
                }
                else {
                    qui shell powershell -Command "Expand-Archive -Path '`outfile'' -DestinationPath '`unzip_dir'' -Force" >nul 2>&1
                }
            }
            else {
                qui shell unzip -o -q "`outfile'" -d "`unzip_dir'" 2>/dev/null
            }
        }
        else {
            if "`decompressor_type'" == "winrar" {
                loc winrar_cmd = subinstr("`decompressor'", "/", "\", .)
                qui shell "`winrar_cmd'" x -o+ -inul "`outfile'" "`unzip_dir'\" >nul 2>&1
            }
            else if "`decompressor_type'" == "7zip_win" {
                loc sevenzip_cmd = subinstr("`decompressor'", "/", "\", .)
                qui shell "`sevenzip_cmd'" x -y -o"`unzip_dir'" "`outfile'" >nul 2>&1
            }
            else if "`decompressor_type'" == "unrar" {
                qui shell unrar x -o+ -inul "`outfile'" "`unzip_dir'/" 2>/dev/null
            }
            else if "`decompressor_type'" == "7zip_unix" {
                qui shell 7z x -y -o"`unzip_dir'" "`outfile'" >/dev/null 2>&1
            }
            else if "`decompressor_type'" == "unar" {
                qui shell unar -q -o "`unzip_dir'" "`outfile'" 2>/dev/null
            }
        }

        sleep 500

        * F. Búsqueda recursiva del .dta
        qui cd "`unzip_dir'"
        loc found_dta ""

        * Nivel 0
        loc dtalist : dir . files "*.dta", respectcase
        foreach f of loc dtalist {
            loc found_dta "`unzip_dir'/`f'"
            continue, break
        }

        * Nivel 1
        if "`found_dta'" == "" {
            loc subdirs : dir . dirs "*", respectcase
            foreach subdir of loc subdirs {
                loc subdtalist : dir "`subdir'" files "*.dta", respectcase
                foreach f of loc subdtalist {
                    loc found_dta "`unzip_dir'/`subdir'/`f'"
                    continue, break
                }
                if "`found_dta'" != "" continue, break

                * Nivel 2
                loc subsubdirs : dir "`subdir'" dirs "*", respectcase
                foreach subsubdir of loc subsubdirs {
                    loc subsubdtalist : dir "`subdir'/`subsubdir'" files "*.dta", respectcase
                    foreach f of loc subsubdtalist {
                        loc found_dta "`unzip_dir'/`subdir'/`subsubdir'/`f'"
                        continue, break
                    }
                    if "`found_dta'" != "" continue, break
                }
                if "`found_dta'" != "" continue, break
            }
        }

        qui cd "`current_dir'"

        if "`found_dta'" == "" {
            di as error "  `y': " as error "[ERROR]" as text " No se encontró .dta en el archivo"
            continue
        }

        * G. Copiar con nombre estandarizado
        copy "`found_dta'" "`final_dta'", replace

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
        if `is_windows' == 1 {
            cap shell rd /s /q "`unzip_dir'"
        }
        else {
            cap shell rm -rf "`unzip_dir'" 2>/dev/null
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

    * --- 6. CARGAR SI SE PIDIÓ ---
    if "`clear'" != "" & "`last_dta'" != "" {
        qui use "`last_dta'", clear
        di as text "Base cargada: " as result "`last_dta'"
    }
    else if "`preserve'" != "" & "`clear'" == "" {
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
    string scalar content, line, best_url, candidate, c
    real scalar fh, j, content_len, score, max_score
    string rowvector extensions
    real scalar ext_i, pos_search, ext_pos, abs_ext_pos, start_pos

    // 1. Leer archivo
    fh = fopen(html_file, "r")
    if (fh < 0) {
        st_global("CASEN_FOUND_URL", "")
        return
    }

    content = ""
    while ((line = fget(fh)) != J(0,0,"")) {
        content = content + line
    }
    fclose(fh)

    // 2. Normalizar a minúsculas
    content = strlower(content)
    content_len = strlen(content)

    best_url = ""
    max_score = -9999

    // 3. Buscar extensiones .zip y .rar
    extensions = (".zip", ".rar")

    for (ext_i = 1; ext_i <= cols(extensions); ext_i++) {
        pos_search = 1

        while (pos_search < content_len) {
            // Buscar extensión
            ext_pos = strpos(substr(content, pos_search, .), extensions[ext_i])
            if (ext_pos == 0) break

            abs_ext_pos = pos_search + ext_pos - 1

            // 4. Back-tracing: encontrar inicio de URL
            candidate = ""
            for (j = abs_ext_pos; j >= max((1, abs_ext_pos - 300)); j--) {
                c = substr(content, j, 1)
                if (c == char(34) | c == "'" | c == "=" | c == ">" | c == " " | c == "(") {
                    start_pos = j + 1
                    candidate = substr(content, start_pos, (abs_ext_pos + strlen(extensions[ext_i]) - 1) - start_pos + 1)
                    break
                }
            }

            // 5. Validar y puntuar
            if (strlen(candidate) > 10) {

                // 6. SCORING SYSTEM
                score = 0

                // Premios
                if (strpos(candidate, "casen") > 0) score = score + 30
                if (strpos(candidate, "stata") > 0) score = score + 100
                if (strpos(candidate, ".dta") > 0) score = score + 80
                if (strpos(candidate, year) > 0) score = score + 50
                if (strpos(candidate, "storage/docs/casen") > 0) score = score + 40

                // Penalizaciones
                if (strpos(candidate, "spss") > 0) score = score - 100
                if (strpos(candidate, ".sav") > 0) score = score - 100
                if (strpos(candidate, "sas") > 0) score = score - 80
                if (strpos(candidate, "csv") > 0) score = score - 50
                if (strpos(candidate, "manual") > 0) score = score - 60
                if (strpos(candidate, "libro") > 0) score = score - 60
                if (strpos(candidate, "metodologia") > 0) score = score - 60
                if (strpos(candidate, "codigos") > 0) score = score - 40
                if (strpos(candidate, "cuestionario") > 0) score = score - 40

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
    }

    st_global("CASEN_FOUND_URL", best_url)
}
end

********************************************************************************
* FIN DEL ARCHIVO
********************************************************************************
