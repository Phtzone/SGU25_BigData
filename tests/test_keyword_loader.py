import unittest
from datetime import date

from scripts.load_keywords_to_postgres import _ensure_date, iter_dataframe_chunks


class KeywordLoaderTests(unittest.TestCase):
    def test_ensure_date_accepts_date(self) -> None:
        value = date(2026, 4, 7)
        self.assertEqual(_ensure_date(value), value)

    def test_ensure_date_rejects_non_date(self) -> None:
        with self.assertRaises(ValueError):
            _ensure_date("2026-04-07")

    def test_iter_dataframe_chunks_splits_rows_by_chunk_size(self) -> None:
        class FakeRow:
            def __init__(self, payload):
                self.payload = payload

            def asDict(self, recursive: bool = True):  # noqa: ARG002
                return self.payload

        class FakeDataFrame:
            @staticmethod
            def toLocalIterator():
                return iter(
                    [
                        FakeRow({"id": 1}),
                        FakeRow({"id": 2}),
                        FakeRow({"id": 3}),
                    ]
                )

        chunks = list(iter_dataframe_chunks(FakeDataFrame(), 2))

        self.assertEqual(chunks, [[{"id": 1}, {"id": 2}], [{"id": 3}]])


if __name__ == "__main__":
    unittest.main()
