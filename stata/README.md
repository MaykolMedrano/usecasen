# usecasen (Stata wrapper)

This folder contains the Stata wrapper command:

- `usecasen.ado`
- `usecasen.sthlp`
- `usecasen.pkg`
- `stata.toc`

## Usage

```stata
usecasen, years(2022) clear
usecasen, years(2017 2020 2022) path("data") replace
```

## Notes

- The command auto-detects the best CASEN file for each year.
- For legacy RAR archives, you need an extractor available in your OS path
  (for example: WinRAR, 7-Zip, unrar, unar, or bsdtar).

## Distribution

Install from a local folder:

```stata
net install usecasen, from("C:/path/to/usecasen/stata")
```

Install from GitHub (public repo):

```stata
net install usecasen, from("https://raw.githubusercontent.com/MaykolMedrano/usecasen/main/stata")
```
