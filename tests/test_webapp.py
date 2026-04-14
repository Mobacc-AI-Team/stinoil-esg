from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = ROOT / "chatgpt_export_to_kb.py"
FLASK_AVAILABLE = importlib.util.find_spec("flask") is not None


def make_demo_kb(tmp_path: Path) -> Path:
    source = tmp_path / "conversations.json"
    output = tmp_path / "kb"
    source.write_text(
        json.dumps(
            [
                {
                    "title": "Klantvraag over CLP",
                    "create_time": 1704067200,
                    "update_time": 1704067200,
                    "mapping": {
                        "user": {
                            "message": {
                                "author": {"role": "user"},
                                "create_time": 1704067200,
                                "content": {"parts": ["Klantvraag over CLP etikettering voor België"]},
                            }
                        },
                        "assistant": {
                            "message": {
                                "author": {"role": "assistant"},
                                "create_time": 1704067260,
                                "content": {"parts": ["Conceptantwoord over etikettering en SDS"]},
                            }
                        },
                    },
                }
            ]
        ),
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(source), "--output", str(output)],
        check=True,
        capture_output=True,
        text=True,
    )
    return output


class FlaskInterfaceTests(unittest.TestCase):
    def test_app_source_contains_flask_interface(self) -> None:
        source = (ROOT / "webapp" / "app.py").read_text(encoding="utf-8")
        self.assertIn("from flask import Flask", source)
        self.assertIn("@app.route(\"/\")", source)
        self.assertIn("@app.route(\"/wetgeving\")", source)
        self.assertIn("@app.route(\"/casussen\")", source)
        self.assertIn("@app.route(\"/casussen/nieuw\", methods=[\"GET\", \"POST\"])", source)
        self.assertIn("@app.route(\"/wetgeving/upload\", methods=[\"GET\", \"POST\"])", source)
        self.assertIn("def derive_categories_from_text", source)
        self.assertIn("@app.route(\"/taxonomie\")", source)
        self.assertIn("from collections import defaultdict", source)
        self.assertIn("def suggest_relevant_regulations", source)

    @unittest.skipUnless(FLASK_AVAILABLE, "Flask is niet lokaal geïnstalleerd")
    def test_dashboard_and_detail_pages_render(self) -> None:
        with TemporaryDirectory() as tmp:
            kb_root = make_demo_kb(Path(tmp))
            os.environ["KB_ROOT"] = str(kb_root)
            from webapp.app import create_app, load_documents

            load_documents.cache_clear()
            app = create_app()
            client = app.test_client()

            dashboard = client.get("/")
            self.assertEqual(dashboard.status_code, 200)
            self.assertIn("Centrale Kennisbank", dashboard.get_data(as_text=True))

            new_case = client.get("/casussen/nieuw")
            self.assertEqual(new_case.status_code, 200)
            self.assertIn("Nieuwe vraag", new_case.get_data(as_text=True))

            wetgeving = client.get("/wetgeving")
            self.assertEqual(wetgeving.status_code, 200)
            self.assertIn("REACH-verordening", wetgeving.get_data(as_text=True))

            health = client.get("/healthz")
            self.assertEqual(health.status_code, 200)
            self.assertIn("kb_exists", health.get_data(as_text=True))

            detail = client.get("/document/02_Vragen/2024/2024-01-01_klantvraag_over_clp_vraag.md")
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Klantvraag over CLP", detail.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
