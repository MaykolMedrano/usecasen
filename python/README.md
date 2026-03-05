# usecasen

**Python Library for CASEN Survey Analysis (Chile)**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/MaykolMedrano/usecasen)

> Professional Python library for downloading and analyzing data from Chile's CASEN Survey (Encuesta de Caracterización Socioeconómica Nacional). Features 100% in-memory operations, intelligent file scoring, and Stata 17+ integration.

**Author**: Maykol Medrano | **Email**: mmedrano2@uc.cl | **GitHub**: [@MaykolMedrano](https://github.com/MaykolMedrano)

---

## Features

- **100% In-Memory**: Download and decompress without disk I/O
- **Intelligent Scoring**: Smart file detection ported from MATA
- **Stata Integration**: Vectorized injection (100x faster than loops)
- **Variable Search**: Efficient search without loading full datasets
- **Codebook Access**: Automatic value label extraction
- **Cache System**: Instant searches after first download
- **Cross-Platform**: Windows, macOS, Linux compatible

---

## Installation

### PyPI (Recommended)

```bash
pip install usecasen
```

### From Source

```bash
git clone https://github.com/MaykolMedrano/usecasen.git
cd usecasen/python
pip install -e .
```

---

## Quick Start

```python
import casen

# Download single year
df = casen.download(2022)
# Returns: DataFrame with 202,231 rows × 918 columns

# Download multiple years
results = casen.download_batch([2017, 2022])
# Returns: {2017: DataFrame, 2022: DataFrame}

# Search variables
results = casen.search("educacion")

# Get value labels
labels = casen.get_labels("region", year=2022)
# Returns: {1: 'Tarapacá', 2: 'Antofagasta', ...}
```

---

## API Reference

### Functions

| Function | Description |
|----------|-------------|
| `download(year)` | Download CASEN data for a single year |
| `download_batch(years)` | Download multiple years at once |
| `search(pattern)` | Search variables by name or label |
| `get_labels(variable, year)` | Get value labels (codebook) |

### Options

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `year` | 1990-2024 | *required* | Survey year |
| `to_stata` | `True` \| `False` | `False` | Inject into Stata memory |
| `verbose` | `True` \| `False` | `True` | Display progress |
| `regex` | `True` \| `False` | `False` | Use regex in search |

---

## Stata 17+ Integration

```stata
python:
import casen
df = casen.download(2022, to_stata=True)
end

describe
summarize

* Search variables from Stata
python:
import casen
results = casen.search("ingreso", verbose=True)
end
```

---

## Package Structure

```
python/
├── casen/
│   ├── __init__.py      # Public API
│   ├── downloader.py    # Download and scraping logic
│   ├── metadata.py      # Search and labels
│   ├── stata_io.py      # Stata integration
│   └── utils.py         # Shared utilities
├── setup.py
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Scoring System (MATA Port)

The library uses an intelligent scoring system to detect the correct files:

**Rewards**: `"casen"` (+30), `"stata"` (+100), `".dta"` (+80), `year` (+50)

**Penalties**: `"spss"` (-100), `"sas"` (-80), `"csv"` (-50), `"manual"` (-60)

---

## Compatibility

| Requirement | Version |
|-------------|---------|
| Python | 3.8+ |
| Stata | 17+ (optional) |
| Pandas | 1.3+ |
| Requests | 2.25+ |
| Pyreadstat | 1.2.7+ (fallback for legacy .dta versions) |
| RAR extractor | WinRAR / 7-Zip / unrar / unar / bsdtar (for older CASEN years) |

---

## Citation

```bibtex
@software{usecasen2025,
  author = {Maykol Medrano},
  title = {usecasen: Python Library for CASEN Survey Analysis},
  version = {1.0.0},
  year = {2025},
  url = {https://github.com/MaykolMedrano/usecasen}
}
```

---

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/NewFeature`
3. Commit changes: `git commit -m 'Add NewFeature'`
4. Push: `git push origin feature/NewFeature`
5. Open Pull Request

---

## Data Source

Data provided by Chile's Ministry of Social Development:
https://observatorio.ministeriodesarrollosocial.gob.cl/

---

## License

MIT License — See [LICENSE](LICENSE) for details.

---

**Version**: 1.0.0 | **Python**: 3.8+ | **Stata**: 17+ (optional)
