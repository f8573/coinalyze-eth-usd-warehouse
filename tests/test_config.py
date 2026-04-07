from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from realtime_querydb.config import Settings


class SettingsFromEnvTests(unittest.TestCase):
    def test_from_env_loads_values_from_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "DATABASE_URL=postgresql://user:pass@localhost:5432/warehouse\n"
                "COINALYZE_API_KEY=test-key\n",
                encoding="utf-8",
            )
            previous_cwd = Path.cwd()
            previous_database_url = os.environ.get("DATABASE_URL")
            previous_api_key = os.environ.get("COINALYZE_API_KEY")
            try:
                os.chdir(temp_dir)
                os.environ.pop("DATABASE_URL", None)
                os.environ.pop("COINALYZE_API_KEY", None)
                settings = Settings.from_env()
            finally:
                os.chdir(previous_cwd)
                _restore_env("DATABASE_URL", previous_database_url)
                _restore_env("COINALYZE_API_KEY", previous_api_key)
            self.assertEqual(settings.database_url, "postgresql://user:pass@localhost:5432/warehouse")
            self.assertEqual(settings.coinalyze_api_key, "test-key")

    def test_from_env_rejects_blank_required_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("DATABASE_URL=\nCOINALYZE_API_KEY=\n", encoding="utf-8")
            previous_cwd = Path.cwd()
            previous_database_url = os.environ.get("DATABASE_URL")
            previous_api_key = os.environ.get("COINALYZE_API_KEY")
            try:
                os.chdir(temp_dir)
                os.environ.pop("DATABASE_URL", None)
                os.environ.pop("COINALYZE_API_KEY", None)
                with self.assertRaisesRegex(RuntimeError, "DATABASE_URL"):
                    Settings.from_env()
            finally:
                os.chdir(previous_cwd)
                _restore_env("DATABASE_URL", previous_database_url)
                _restore_env("COINALYZE_API_KEY", previous_api_key)


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
        return
    os.environ[name] = value
