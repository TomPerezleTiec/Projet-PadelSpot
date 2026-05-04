from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def notebook(self) -> Path:
        return self.root / "padelspot.ipynb"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def docs_dir(self) -> Path:
        return self.root / "docs"

    @property
    def src_dir(self) -> Path:
        return self.root / "src"

    @property
    def conf_dir(self) -> Path:
        return self.root / "conf"

    @property
    def pyproject(self) -> Path:
        return self.root / "pyproject.toml"

    @property
    def generated_pipeline(self) -> Path:
        return self.src_dir / "padelspot" / "pipelines" / "padelspot_pipeline_from_notebook.py"


def get_project_paths() -> ProjectPaths:
    return ProjectPaths(root=Path(__file__).resolve().parents[2])
