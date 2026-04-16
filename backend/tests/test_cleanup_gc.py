import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.cleanup import garbage_collect_artifact_files


class CleanupGarbageCollectionTests(unittest.TestCase):
    def test_gc_removes_only_unreferenced_artifact_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            managed_dir = root / "models" / "project_1"
            managed_dir.mkdir(parents=True, exist_ok=True)
            referenced = managed_dir / "predictions_keep.parquet"
            orphan = managed_dir / "predictions_orphan.parquet"
            ignored = managed_dir / "notes.txt"
            referenced.write_text("keep", encoding="utf-8")
            orphan.write_text("remove", encoding="utf-8")
            ignored.write_text("ignore", encoding="utf-8")

            with (
                patch("app.services.cleanup._iter_managed_storage_roots", return_value=[root]),
                patch("app.services.cleanup._collect_referenced_artifact_paths", return_value={referenced.resolve()}),
            ):
                result = garbage_collect_artifact_files(db=object())

            self.assertTrue(referenced.exists())
            self.assertFalse(orphan.exists())
            self.assertTrue(ignored.exists())
            self.assertEqual(result["removed_files"], 1)


if __name__ == "__main__":
    unittest.main()
