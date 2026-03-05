import io
import unittest
from unittest import mock

import pandas as pd

from casen.downloader import CasenDownloader
from casen import metadata


class MetadataTests(unittest.TestCase):
    @staticmethod
    def _build_dta_with_labels() -> io.BytesIO:
        df = pd.DataFrame({"region": [1, 2, 1], "edad": [30, 40, 20]})
        buffer = io.BytesIO()
        df.to_stata(
            buffer,
            write_index=False,
            value_labels={"region": {1: "Norte", 2: "Sur"}},
        )
        buffer.seek(0)
        return buffer

    @mock.patch("casen.metadata._download_year_payload")
    def test_extract_value_labels_from_generated_dta(self, mock_download_payload: mock.Mock) -> None:
        mock_download_payload.return_value = self._build_dta_with_labels()

        downloader = CasenDownloader(verbose=False)
        labels = metadata._extract_value_labels(
            year=2022,
            variable="region",
            downloader=downloader,
            verbose=False,
        )

        self.assertIsNotNone(labels)
        assert labels is not None
        self.assertIn("Norte", labels.values())
        self.assertIn("Sur", labels.values())

    @mock.patch("casen.metadata._download_year_payload")
    def test_extract_metadata_from_generated_dta(self, mock_download_payload: mock.Mock) -> None:
        mock_download_payload.return_value = self._build_dta_with_labels()

        downloader = CasenDownloader(verbose=False)
        extracted = metadata._extract_metadata(year=2022, downloader=downloader, verbose=False)

        self.assertIsNotNone(extracted)
        assert extracted is not None
        self.assertIn("region", extracted)
        self.assertIn("edad", extracted)

    @mock.patch("casen.metadata._download_year_payload")
    @mock.patch("casen.metadata.pd.read_stata", side_effect=Exception("legacy not supported by pandas"))
    def test_extract_metadata_fallback_pyreadstat(self, _mock_read_stata: mock.Mock,
                                                  mock_download_payload: mock.Mock) -> None:
        mock_download_payload.return_value = self._build_dta_with_labels()

        downloader = CasenDownloader(verbose=False)
        extracted = metadata._extract_metadata(year=2022, downloader=downloader, verbose=False)

        self.assertIsNotNone(extracted)
        assert extracted is not None
        self.assertIn("region", extracted)

    @mock.patch("casen.metadata._download_year_payload")
    @mock.patch("casen.metadata.StataReader", side_effect=Exception("legacy reader error"))
    def test_extract_value_labels_fallback_pyreadstat(self, _mock_reader: mock.Mock,
                                                      mock_download_payload: mock.Mock) -> None:
        mock_download_payload.return_value = self._build_dta_with_labels()

        downloader = CasenDownloader(verbose=False)
        labels = metadata._extract_value_labels(
            year=2022,
            variable="region",
            downloader=downloader,
            verbose=False,
        )

        self.assertIsNotNone(labels)
        assert labels is not None
        self.assertIn("Norte", labels.values())


if __name__ == "__main__":
    unittest.main()
