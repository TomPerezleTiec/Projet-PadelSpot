from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NOTEBOOK = PROJECT_ROOT / "padelspot.ipynb"
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "src" / "padelspot" / "pipelines" / "padelspot_pipeline_from_notebook.py"
)


def _comment_markdown_cell(source: str) -> list[str]:
    lines = ["# " + line if line else "#" for line in source.splitlines()]
    return lines or ["#"]


def _sanitize_code_line(line: str) -> str:
    stripped = line.lstrip()
    if stripped.startswith("%") or stripped.startswith("!"):
        return "# NOTEBOOK_MAGIC: " + stripped
    return line.rstrip("\n")


def _render_cells(cells: Iterable[dict], include_markdown: bool) -> str:
    rendered: list[str] = []

    for index, cell in enumerate(cells, start=1):
        cell_type = cell.get("cell_type", "unknown")
        source = "".join(cell.get("source", []))
        rendered.append(f"# ===== Cell {index:03d} | {cell_type} =====")

        if cell_type == "markdown":
            if include_markdown:
                rendered.extend(_comment_markdown_cell(source))
            else:
                rendered.append("# markdown omitted")
        elif cell_type == "code":
            code_lines = source.splitlines()
            if code_lines:
                rendered.extend(_sanitize_code_line(line) for line in code_lines)
            else:
                rendered.append("pass")
        else:
            rendered.append("# unsupported cell type")

        rendered.append("")

    return "\n".join(rendered).rstrip() + "\n"


def export_notebook(notebook_path: Path, output_path: Path, include_markdown: bool) -> None:
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    cells = notebook.get("cells", [])

    header = [
        '"""',
        "Generated from padelspot.ipynb.",
        "",
        "This file is intentionally generated from the notebook so the pipeline",
        "can be versioned and reviewed as plain Python.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "\n".join(header) + _render_cells(cells, include_markdown),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export notebook code cells into a versionable Python pipeline file."
    )
    parser.add_argument(
        "--notebook",
        type=Path,
        default=DEFAULT_NOTEBOOK,
        help="Path to the source notebook.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to the generated Python file.",
    )
    parser.add_argument(
        "--include-markdown",
        action="store_true",
        help="Include markdown cells as comments in the generated file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    export_notebook(
        notebook_path=args.notebook.resolve(),
        output_path=args.output.resolve(),
        include_markdown=args.include_markdown,
    )
    print(f"Notebook exported to: {args.output}")


if __name__ == "__main__":
    main()
