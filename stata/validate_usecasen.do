version 14.0
clear all
set more off
capture log close _all

/*
Validate usecasen package installation and core behavior.

Usage (from repo root):
    do stata/validate_usecasen.do
    do stata/validate_usecasen.do full

Mode:
    default -> smoke years (2022)
    full    -> heavier validation (1990 2006 2017 2022)
*/

args mode

local cwd `"`c(pwd)'"'
local install_from ""

* If running inside stata/ folder, install from current dir.
capture confirm file "`cwd'/usecasen.ado"
if _rc == 0 {
    local install_from `"`cwd'"'
}
else {
    local install_from `"`cwd'/stata'"'
}

capture confirm file "`install_from'/usecasen.ado"
if _rc != 0 {
    di as error "[FATAL] No se encontro usecasen.ado en: `install_from'"
    exit 601
}

local data_path `"`cwd'/.tmp_usecasen_validation'"'
capture mkdir "`data_path'"

local log_file `"`cwd'/usecasen_validation.log'"'
log using "`log_file'", text replace

di as text "============================================================"
di as text " VALIDACION USECASEN (Stata)"
di as text " install_from: `install_from'"
di as text " data_path   : `data_path'"
di as text " mode        : `mode'"
di as text "============================================================"

* 1) Install package from local folder
capture noisily net install usecasen, from("`install_from'") replace
if _rc != 0 {
    di as error "[FATAL] net install fallo con rc=`_rc'"
    log close
    exit _rc
}

capture noisily which usecasen
if _rc != 0 {
    di as error "[FATAL] which usecasen fallo con rc=`_rc'"
    log close
    exit _rc
}

local failed 0
local years "2022"
if "`mode'" == "full" {
    local years "1990 2006 2017 2022"
}

* 2) Main year tests
foreach y of local years {
    di as text ""
    di as text "[TEST] Descarga y validacion anio `y'"

    capture noisily usecasen, years(`y') path("`data_path'") replace retries(2) timeout(180)
    local rc = _rc
    if `rc' != 0 {
        di as error "  [FAIL] usecasen fallo en anio `y' (rc=`rc')"
        local failed = 1
        continue
    }

    capture confirm file "`data_path'/casen_dta/casen_`y'.dta"
    if _rc != 0 {
        di as error "  [FAIL] No existe archivo final casen_`y'.dta"
        local failed = 1
        continue
    }

    quietly use "`data_path'/casen_dta/casen_`y'.dta", clear
    local nobs = _N
    local nvars = c(k)
    di as result "  [OK] `y': `nobs' obs x `nvars' vars"
}

* 3) debugscore smoke
di as text ""
di as text "[TEST] debugscore (anio 2022)"
capture noisily usecasen, years(2022) path("`data_path'") replace retries(1) timeout(180) debugscore
if _rc != 0 {
    di as error "  [FAIL] debugscore fallo (rc=`_rc')"
    local failed = 1
}
else {
    di as result "  [OK] debugscore"
}

* 4) strict behavior smoke (should stop on bad year and return rc=459)
di as text ""
di as text "[TEST] strict mode (2022 2099)"
capture noisily usecasen, years(2022 2099) path("`data_path'") replace strict retries(1) timeout(120)
local strict_rc = _rc
if `strict_rc' == 459 {
    di as result "  [OK] strict retorno rc=459 como esperado"
}
else {
    di as error "  [FAIL] strict retorno rc=`strict_rc' (esperado 459)"
    local failed = 1
}

di as text ""
di as text "============================================================"
if `failed' == 0 {
    di as result " RESULTADO FINAL: OK"
    di as text " Log: `log_file'"
    di as text "============================================================"
    log close
    exit 0
}
else {
    di as error " RESULTADO FINAL: FAIL"
    di as text " Log: `log_file'"
    di as text "============================================================"
    log close
    exit 9
}
