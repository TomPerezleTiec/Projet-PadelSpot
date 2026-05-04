from __future__ import annotations

import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _extract_dvc_stage_names(path: Path) -> set[str]:
    stage_pattern = re.compile(r"^\s{2}([a-z0-9_]+):\s*$")
    stage_names: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = stage_pattern.match(line)
        if match:
            stage_name = match.group(1)
            if stage_name.startswith("stage_") or stage_name == "build_stage_scripts":
                stage_names.add(stage_name)
    return stage_names


class ProjectValidationTests(unittest.TestCase):
    def test_dvc_lock_covers_all_declared_stages(self) -> None:
        declared_stages = _extract_dvc_stage_names(PROJECT_ROOT / "dvc.yaml")
        locked_stages = _extract_dvc_stage_names(PROJECT_ROOT / "dvc.lock")

        self.assertTrue(
            declared_stages.issubset(locked_stages),
            msg=(
                "Some declared DVC stages are missing from dvc.lock: "
                f"{sorted(declared_stages - locked_stages)}"
            ),
        )

    def test_copier_template_is_present(self) -> None:
        template_root = PROJECT_ROOT / "scaffolding" / "copier-template"

        self.assertTrue(template_root.is_dir(), "Copier template directory is missing")
        self.assertTrue((template_root / "copier.yml").is_file(), "copier.yml is missing")
        self.assertTrue((template_root / "README.md").is_file(), "template README is missing")


if __name__ == "__main__":
    unittest.main()