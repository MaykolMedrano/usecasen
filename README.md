<div align="center">
  
# usecasen
  
  **La suite profesional definitiva para descarga, recodificación y análisis de la Encuesta CASEN (Chile) en Python y Stata.**

  [![PyPI Version](https://img.shields.io/pypi/v/usecasen?style=flat-square&color=blue)](https://pypi.org/project/usecasen/)
  [![Language Support](https://img.shields.io/badge/Python_3.8%2B_%7C_Stata_14%2B-blue?style=flat-square)](https://www.python.org/)
  [![Tests passing](https://img.shields.io/github/actions/workflow/status/MaykolMedrano/usecasen/python-app.yml?branch=main&style=flat-square)](https://github.com/MaykolMedrano/usecasen/actions)
  [![Downloads](https://img.shields.io/pypi/dm/usecasen?style=flat-square&color=blue)](https://pypi.org/project/usecasen/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
  [Ver Documentación Python](python/README.md) • [Ver Documentación Stata](stata/README.md) • [Reportar Bug](https://github.com/MaykolMedrano/usecasen/issues)
</div>

---

## Sobre el Proyecto

**`usecasen`** es una herramienta unificada para **Python** y **Stata** que automatiza la búsqueda, descarga y procesamiento de los microdatos de la **Encuesta de Caracterización Socioeconómica Nacional (CASEN)** de Chile.

Cuenta con un sistema de **scoring inteligente** integrado que detecta y extrae de forma automática los archivos oficiales correctos desde los servidores del Ministerio de Desarrollo Social y Familia (MDSF), resolviendo inconsistencias de encoding heredadas y operando de forma transparente sobre múltiples formatos de compresión (`.dta`, `.zip`, `.rar`).

### Características Principales

- **Smart Scoring & Fallbacks**: Motor de búsqueda que evalúa y aísla el archivo `.dta` principal entre cientos de documentos y anexos disponibles.
- **Encoding Automático**: Traduce de forma nativa las bases anteriores a 2013 (latin1/ISO-8859) a UTF-8 para garantizar compatibilidad con versiones modernas de software.
- **Soporte Multi-Compresión**: Trabaja con volúmenes masivos comprimidos en formatos como RAR y ZIP (ej. CASEN 2017) invocando automáticamente las herramientas del sistema (7z, winrar, powershell, unar, bsdtar).
- **Procesamiento In-Memory (Python)**: Configurado para descargar y cargar los datos directamente en memoria RAM, eliminando cuellos de botella por operaciones de I/O en disco.
- **Caché Inteligente**: Guarda localmente el dataset tras la primera consulta, reduciendo el tiempo de carga a milisegundos en peticiones subsecuentes.

---

## 1. Paquete Python (`python/casen`)

La librería de Python es ideal para interactuar con CASEN de manera exploratoria, rápida, y exportando DataFrames, logrando inyecciones directas en `pandas`.

**Instalación Rápida:**

```bash
pip install usecasen
```

**Uso Exploratorio Rápido:**

```python
import casen

# Descargar datos (con logs de progreso)
df_2022 = casen.download(2022)

# Buscar en los metadatos (etiquetas de variable) en todo el archivo 2022, SIN descargar
resultados = casen.search("educacion")

# Consultar el diccionario (codebook) nativo de la CASEN
diccionario_regiones = casen.get_labels("region", 2022)
print(diccionario_regiones)
# {1: 'Tarapacá', 2: 'Antofagasta', ...}
```

> _Ver guía completa de Python y su integración a Stata (sfi) en:_ [`python/README.md`](python/README.md)

---

## 2. Wrapper para Stata (`stata/usecasen.ado`)

Comando robusto, compatible desde Stata 14 hasta 19, diseñado para procesar y consolidar masivamente datasets, logrando la limpieza nativa.

**Instalación Rápida:**

```stata
net install usecasen, from("https://raw.githubusercontent.com/MaykolMedrano/usecasen/master/stata") replace
```

**Uso Clásico:**

```stata
* Descargar/cargar el último año disponible con limpieza en memoria
usecasen, years(2022) clear

* Descarga masiva para hacer paneles (descarga en /data y reemplaza)
usecasen, years(2006 2017 2020 2022) path("data") replace

* Modo hardcore con logs y timeouts (para redes lentas o debug)
usecasen, years(1990) retries(3) timeout(600) debugscore clear
```

> _Ver guía completa, comandos y ayudas de Stata en:_ [`stata/README.md`](stata/README.md)

---

## Estructura del Repositorio

```text
usecasen
 |- python/             # API PyPI, Core in-memory, Metadata Scanner, Tests
 |- stata/              # Wrapper Stata (.ado/.sthlp/.pkg), Fallback extracts
 |- .github/workflows/  # CI/CD (Pytest Actions)
 `- README.md           # This file
```

---

## Licencia & Citas

El código de este producto está licenciado nativamente bajo **MIT License**, mira [LICENSE](python/LICENSE) para detalles completos.

Si este proyecto ha acelerado substancialmente tu investigación o tesis, puedes referenciar el repositorio:

```bibtex
@software{usecasen2026,
  author = {Medrano, Maykol},
  title = {usecasen: Herramientas Python y Stata para la Encuesta CASEN},
  version = {1.0.0},
  year = {2026},
  publisher = {GitHub},
  url = {https://github.com/MaykolMedrano/usecasen}
}
```

> **Aviso Legal de Datos**:
Los microdatos que descarga esta herramienta son propiedad intelectual y pública del Ministerio de Desarrollo Social y Familia (MDSF) del Gobierno de Chile [Observatorio Social](https://observatorio.ministeriodesarrollosocial.gob.cl/).
