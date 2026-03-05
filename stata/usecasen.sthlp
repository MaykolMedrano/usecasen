{smcl}
{* *! version 3.2.0 24feb2026}{...}
{title:Title}

{p2colset 5 20 22 2}{...}
{p2col:{hi:usecasen}}Descarga y prepara microdatos CASEN (Chile){p_end}
{p2colreset}{...}

{marker syntax}{...}
{title:Syntax}

{p 8 17 2}
{cmd:usecasen},
{cmd:years(}{it:numlist}{cmd:)}
[{cmd:path(}{it:string}{cmd:)} {cmd:replace} {cmd:clear} {cmd:preserve} {cmd:noraw}
{cmd:strict} {cmd:retries(}{it:#}{cmd:)} {cmd:timeout(}{it:#}{cmd:)} {cmd:debugscore}]

{marker description}{...}
{title:Description}

{pstd}
{cmd:usecasen} descarga archivos CASEN desde el sitio oficial del Ministerio de
Desarrollo Social y Familia, detecta automáticamente el enlace más probable,
extrae el archivo de datos y guarda una versión estandarizada como
{cmd:casen_YYYY.dta}.

{pstd}
El comando crea/usa esta estructura dentro de {cmd:path()}:
{break}{cmd:casen_raw/} archivos descargados ({cmd:.dta}, {cmd:.zip}, {cmd:.rar})
{break}{cmd:casen_dta/} archivo final estandarizado por año
{break}{cmd:casen_temp/} extracción temporal

{marker options}{...}
{title:Options}

{phang}
{cmd:years(}{it:numlist}{cmd:)} lista de años CASEN a procesar. Esta opción es requerida.

{phang}
{cmd:path(}{it:string}{cmd:)} directorio base de trabajo. Default: directorio actual.

{phang}
{cmd:replace} vuelve a descargar/reprocesar aun si {cmd:casen_YYYY.dta} ya existe.
Sin {cmd:replace}, el comando usa cache local.

{phang}
{cmd:clear} carga en memoria la última base descargada/procesada.

{phang}
{cmd:preserve} guarda la base actual al inicio y la restaura al final cuando
{cmd:clear} no está especificado.

{phang}
{cmd:noraw} elimina {cmd:casen_raw/} al finalizar.

{phang}
{cmd:strict} aborta el comando al primer año con error (default: continúa con el siguiente año).

{phang}
{cmd:retries(}{it:#}{cmd:)} cantidad de intentos de descarga por recurso (HTML y archivo).
Default: {cmd:2}. Mínimo: {cmd:1}.

{phang}
{cmd:timeout(}{it:#}{cmd:)} timeout de descarga en segundos cuando está disponible en {cmd:copy}.
Default: {cmd:120}. Use {cmd:0} para desactivar.

{phang}
{cmd:debugscore} muestra URL elegida, intentos de descarga y puntajes de candidatos {cmd:.dta}.

{marker remarks}{...}
{title:Remarks}

{pstd}
Compatibilidad de compresión:

{phang2}
1. {cmd:.dta} directo: soportado sin extractor externo.

{phang2}
2. {cmd:.zip}: soportado en Windows (PowerShell o 7-Zip) y Unix/macOS (unzip, 7z, unar o bsdtar).

{phang2}
3. {cmd:.rar}: requiere extractor externo (por ejemplo WinRAR, 7-Zip, unrar, unar o bsdtar).

{pstd}
Para años anteriores a 2013 el comando intenta traducción de encoding latin1 con
herramientas Unicode de Stata.

{marker examples}{...}
{title:Examples}

{phang2}{cmd:. usecasen, years(2022) clear}

{phang2}{cmd:. usecasen, years(2017 2020 2022) path("data") replace}

{phang2}{cmd:. usecasen, years(1990 2006) path("data") noraw}

{phang2}{cmd:. usecasen, years(1990 2006 2022) strict retries(3) timeout(180) debugscore}

{marker author}{...}
{title:Author}

{pstd}
Maykol Medrano
{break}Pontificia Universidad Católica de Chile
{break}Email: mmedrano2@uc.cl
{break}GitHub: https://github.com/MaykolMedrano

{marker source}{...}
{title:Data source}

{pstd}
https://observatorio.ministeriodesarrollosocial.gob.cl/
