# usecasen

Herramientas para descargar y analizar datos de la Encuesta CASEN (Chile).

## Componentes

### 1. usecasen.ado (Stata)
Comando Stata para descarga automática de datos CASEN con sistema inteligente de detección de archivos.

**Características:**
- Descarga automática desde el sitio oficial del Ministerio de Desarrollo Social
- Sistema de scoring inteligente para detectar archivos correctos (MATA)
- Soporte multiplataforma (Windows/Unix/macOS)
- Detección automática de descompresores (WinRAR, 7-Zip, unrar)
- Sistema de caché para evitar descargas repetidas
- Conversión automática de encoding para años antiguos

**Uso:**
```stata
usecasen, years(2022) clear
usecasen, years(2017 2020 2022) path("data") replace
```

### 2. usecasen-py (Python)
Librería Python profesional para descarga y análisis de datos CASEN.

**Características:**
- 100% operaciones en memoria (sin uso de disco)
- Sistema de scoring inteligente (portado desde MATA)
- Integración vectorizada con Stata 17+
- Búsqueda eficiente de variables sin cargar datos completos
- Extracción de etiquetas de valores (codebook)
- Sistema de caché para búsquedas instantáneas

**Instalación:**
```bash
pip install usecasen
```

**Uso:**
```python
import casen

# Descargar datos
df = casen.download(2022)

# Buscar variables
results = casen.search("educacion")

# Obtener etiquetas
labels = casen.get_labels("region", 2022)
```

Ver documentación completa en [usecasen-py/README.md](usecasen-py/README.md)

## Autor

**Maykol Medrano**
Pontificia Universidad Católica de Chile
Email: mmedrano2@uc.cl
GitHub: [@MaykolMedrano](https://github.com/MaykolMedrano)

## Licencia

MIT License - Ver [LICENSE](usecasen-py/LICENSE) para más detalles.

## Datos

Los datos son propiedad del Ministerio de Desarrollo Social y Familia de Chile y están disponibles públicamente en:
https://observatorio.ministeriodesarrollosocial.gob.cl/

## Contribuciones

Issues y Pull Requests son bienvenidos en https://github.com/MaykolMedrano/usecasen
