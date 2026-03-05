import io
import unittest
import zipfile
from unittest import mock

import pandas as pd

from casen.downloader import CasenDownloader


class DownloaderScoringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.downloader = CasenDownloader(verbose=False)

    def test_scoring_prefers_main_dataset_2017(self) -> None:
        main_url = "storage/docs/casen/2017/casen_2017.dta.zip"
        aux_url = "storage/docs/casen/2017/Casen2017_factor_raking_deciles_y_quintil STATA.dta.zip"

        main_score = self.downloader._calculate_score(main_url, "2017")
        aux_score = self.downloader._calculate_score(aux_url, "2017")

        self.assertGreater(main_score, aux_score)

    def test_scoring_prefers_main_dataset_1990(self) -> None:
        main_url = "storage/docs/casen/1990/casen1990stata.rar"
        aux_url = "storage/docs/casen/1990/ingresos_originales_casen_1990_stata.rar"

        main_score = self.downloader._calculate_score(main_url, "1990")
        aux_score = self.downloader._calculate_score(aux_url, "1990")

        self.assertGreater(main_score, aux_score)

    def test_get_best_link_prefers_main_href(self) -> None:
        html = """
        <html><body>
          <a href="storage/docs/casen/2017/Casen2017_factor_raking_deciles_y_quintil STATA.dta.zip">aux</a>
          <a href="storage/docs/casen/2017/casen_2017.dta.zip">main</a>
        </body></html>
        """
        best = self.downloader._get_best_link(html, "2017")

        self.assertEqual(best, "storage/docs/casen/2017/casen_2017.dta.zip")


class DownloaderExtractionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.downloader = CasenDownloader(verbose=False)

    @staticmethod
    def _to_dta_bytes(df: pd.DataFrame) -> bytes:
        buffer = io.BytesIO()
        df.to_stata(buffer, write_index=False)
        return buffer.getvalue()

    def test_extract_and_load_from_zip_uses_best_candidate(self) -> None:
        main_df = pd.DataFrame({"x": [1, 2, 3]})
        aux_df = pd.DataFrame({"x": [9]})

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "Casen2017_factor_raking_deciles_y_quintil STATA.dta",
                self._to_dta_bytes(aux_df),
            )
            zf.writestr("casen_2017.dta", self._to_dta_bytes(main_df))

        zip_buffer.seek(0)
        result = self.downloader._extract_and_load_dta(zip_buffer, 2017)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.shape, (3, 1))
        self.assertListEqual(result["x"].tolist(), [1, 2, 3])

    @mock.patch("casen.downloader.pd.read_stata", side_effect=Exception("legacy not supported by pandas"))
    def test_extract_and_load_fallbacks_to_pyreadstat(self, _mock_read_stata: mock.Mock) -> None:
        source_df = pd.DataFrame({"x": [10, 20]})
        dta_bytes = self._to_dta_bytes(source_df)
        result = self.downloader._extract_and_load_dta(io.BytesIO(dta_bytes), 2017)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.shape, (2, 1))
        self.assertListEqual(result["x"].tolist(), [10, 20])


if __name__ == "__main__":
    unittest.main()
