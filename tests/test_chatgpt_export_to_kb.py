from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "chatgpt_export_to_kb.py"


def conversation_payload(title: str, create_time: int, text: str) -> dict:
    return {
        "title": title,
        "create_time": create_time,
        "update_time": create_time,
        "mapping": {
            "user-node": {
                "message": {
                    "author": {"role": "user"},
                    "create_time": create_time,
                    "content": {"parts": [text]},
                }
            },
            "assistant-node": {
                "message": {
                    "author": {"role": "assistant"},
                    "create_time": create_time + 1,
                    "content": {"parts": [f"Antwoord op: {text}"]},
                }
            },
        },
    }


class ChatGptExportScriptTests(unittest.TestCase):
    def run_script(self, input_path: Path, output_path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT_PATH), str(input_path), "--output", str(output_path)],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_import_creates_case_bundle_with_unique_filenames(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "conversations.json"
            output = tmp_path / "kb"
            source.write_text(
                json.dumps(
                    [
                        conversation_payload("Audit gesprek", 1704067200, "Eerste vraag"),
                        conversation_payload("Audit gesprek", 1704067200, "Tweede vraag"),
                    ]
                ),
                encoding="utf-8",
            )

            result = self.run_script(source, output)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            vraag_dir = output / "02_Vragen" / "2024"
            beoordeling_dir = output / "03_Beoordelingen" / "2024"
            antwoord_dir = output / "04_Antwoorden" / "2024"

            self.assertEqual(
                sorted(path.name for path in vraag_dir.glob("*.md")),
                [
                    "2024-01-01_audit_gesprek_2_vraag.md",
                    "2024-01-01_audit_gesprek_vraag.md",
                ],
            )
            self.assertTrue((beoordeling_dir / "2024-01-01_audit_gesprek_beoordeling.md").exists())
            self.assertTrue((antwoord_dir / "2024-01-01_audit_gesprek_antwoord.md").exists())

    def test_second_import_preserves_existing_indexes(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            output = tmp_path / "kb"

            first_source = tmp_path / "conversations_first.json"
            first_source.write_text(
                json.dumps([conversation_payload("Eerste gesprek", 1704067200, "Klantvraag over SDS")]),
                encoding="utf-8",
            )
            first_result = self.run_script(first_source, output)
            self.assertEqual(first_result.returncode, 0, msg=first_result.stderr)

            second_source = tmp_path / "conversations_second.json"
            second_source.write_text(
                json.dumps([conversation_payload("Tweede gesprek", 1735689600, "Leveranciersvraag over ADR")]),
                encoding="utf-8",
            )
            second_result = self.run_script(second_source, output)
            self.assertEqual(second_result.returncode, 0, msg=second_result.stderr)

            vragen_index = (output / "08_Index" / "index_vragen.md").read_text(encoding="utf-8")
            antwoorden_index = (output / "08_Index" / "index_antwoorden.md").read_text(encoding="utf-8")

            self.assertIn("Eerste gesprek", vragen_index)
            self.assertIn("Tweede gesprek", vragen_index)
            self.assertIn("Eerste gesprek", antwoorden_index)
            self.assertIn("Tweede gesprek", antwoorden_index)

    def test_static_compliance_structure_is_created(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "conversations.json"
            output = tmp_path / "kb"
            source.write_text(
                json.dumps([conversation_payload("Vraag over CLP", 1704067200, "Klantvraag over etikettering")]),
                encoding="utf-8",
            )

            result = self.run_script(source, output)

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((output / "01_Wetgeving" / "eu").exists())
            self.assertTrue((output / "06_Locaties").exists())
            self.assertTrue((output / "07_Templates" / "template_wetgeving.md").exists())
            self.assertTrue((output / "08_Index" / "index_wetgeving.md").exists())
            self.assertTrue((output / "09_Workflows" / "import_procedure.md").exists())


if __name__ == "__main__":
    unittest.main()
