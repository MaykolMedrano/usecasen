"""
Setup configuration for CASEN library
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name='usecasen',
    version='1.0.0',
    author='Maykol Medrano',
    author_email='mmedrano2@uc.cl',
    description='Modern Python library for downloading and analyzing Chilean CASEN survey data',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/MaykolMedrano/usecasen',
    project_urls={
        'Bug Tracker': 'https://github.com/MaykolMedrano/usecasen/issues',
        'Documentation': 'https://github.com/MaykolMedrano/usecasen#readme',
        'Source Code': 'https://github.com/MaykolMedrano/usecasen',
    },
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Sociology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'pandas>=1.3.0',
        'requests>=2.25.0',
        'beautifulsoup4>=4.9.0',
        'tqdm>=4.60.0',
        'pyreadstat>=1.2.7',
    ],
    extras_require={
        'stata': [],  # sfi is only available in Stata environment, not installable via pip
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=3.0.0',
            'black>=22.0.0',
            'flake8>=4.0.0',
        ],
    },
    keywords=[
        'casen',
        'chile',
        'survey',
        'socioeconomic',
        'data',
        'statistics',
        'stata',
        'research',
    ],
    license='MIT',
    zip_safe=False,
    include_package_data=True,
)
