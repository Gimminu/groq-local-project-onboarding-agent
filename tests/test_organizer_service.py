from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.organizer_service import FolderOrganizer
from app.organizer_types import OrganizerConfig


class OrganizerServiceTests(unittest.TestCase):
    def build_config(self, source_root: Path) -> OrganizerConfig:
        return OrganizerConfig(
            source_root=source_root,
            target_root=source_root,
            output_dir=source_root / "outputs",
            min_age_seconds=0,
            provider="heuristic",
        )

    def test_watch_moves_pdf_to_inbox(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            pdf_path = root / "lecture.pdf"
            pdf_path.write_text("sample", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("watch")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("move", decision.action)
            self.assertEqual((root / "00_Inbox" / "lecture.pdf").resolve(), decision.destination_path)

    def test_watch_skips_incomplete_download_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            partial = root / "Unconfirmed 12345.crdownload"
            partial.write_text("partial", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("watch")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("skip", decision.action)
            self.assertIn("다운로드가 끝나지 않은 임시 항목", decision.reason)

    def test_watch_skips_zero_byte_placeholder_while_download_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            placeholder = root / "Vysor-mac-5.0.7.dmg"
            placeholder.write_bytes(b"")
            partial = root / "Unconfirmed 582853.crdownload"
            partial.write_text("partial", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("watch")

            self.assertEqual(2, len(plan.decisions))
            decisions = {item.source_path.name: item for item in plan.decisions}
            self.assertEqual("skip", decisions["Vysor-mac-5.0.7.dmg"].action)
            self.assertEqual("skip", decisions["Unconfirmed 582853.crdownload"].action)

    def test_plan_moves_project_directory_without_touching_inside(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            project_dir = root / "demo-app"
            project_dir.mkdir()
            (project_dir / "package.json").write_text("{}", encoding="utf-8")
            (project_dir / "src").mkdir()
            (project_dir / "src" / "index.js").write_text("console.log('x')", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("plan")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("01_Projects", decision.para_root)
            self.assertEqual((root / "01_Projects" / "demo-app").resolve(), decision.destination_path)

    def test_plan_detects_platformio_style_folder_as_project_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            project_dir = root / "예제 소스코드"
            project_dir.mkdir()
            (project_dir / ".pio").mkdir()
            (project_dir / "platformio.ini").write_text("[env:test]\nplatform = atmelavr", encoding="utf-8")
            lab_dir = project_dir / "lab01"
            lab_dir.mkdir()
            (lab_dir / "main.ino").write_text("void setup() {}\nvoid loop() {}\n", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("plan")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("move", decision.action)
            self.assertEqual((root / "01_Projects" / "예제 소스코드").resolve(), decision.destination_path)

    def test_project_documents_use_explicit_documents_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            doc = root / "capstone_notes.pdf"
            doc.write_text("sample", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("plan")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual(
                (root / "01_Projects" / "capstone_notes" / "Documents" / "capstone_notes.pdf").resolve(),
                decision.destination_path,
            )

    def test_code_file_is_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            code_file = root / "script.py"
            code_file.write_text("print('hello')", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("plan")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("manual_review", decision.status)

    def test_hwpx_file_requires_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            doc = root / "도전제안서 양식(루키).hwpx"
            doc.write_text("sample", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            plan = organizer.build_plan("plan")

            self.assertEqual(1, len(plan.decisions))
            decision = plan.decisions[0]
            self.assertEqual("manual_review", decision.status)

    def test_protected_directory_is_skipped_from_top_level_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            protected = root / "protected"
            protected.mkdir()
            (protected / "note.pdf").write_text("x", encoding="utf-8")

            organizer = FolderOrganizer(self.build_config(root))
            organizer.protected_paths.add(protected.resolve())

            plan = organizer.build_plan("plan")
            self.assertEqual([], plan.decisions)


if __name__ == "__main__":
    unittest.main()
