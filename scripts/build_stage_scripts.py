from __future__ import annotations

import json
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = PROJECT_ROOT / "padelspot.ipynb"
OUTPUT_DIR = PROJECT_ROOT / "src" / "padelspot" / "jobs"

SHARED_PATH_CONSTANTS = {
    "OUTPUT_DVF": "/home/jovyan/work/data/output/dvf_clean/",
    "OUTPUT_FILOSOFI": "/home/jovyan/work/data/output/filosofi_clean/",
    "OUTPUT_CONCURRENCE": "/home/jovyan/work/data/output/concurrence_padel/",
    "output_path_access": "/home/jovyan/work/data/output/accessibilite_clean/",
    "OUTPUT_TRENDS": "/home/jovyan/work/data/output/trends_joined/",
    "OUTPUT_DASH": "/home/jovyan/work/data/dash_ready/",
}


SECTION_PATTERN = re.compile(r"^##\s+(\d+)\s*[–-]\s+(.+)$")


def sanitize_code_line(line: str) -> str:
    stripped = line.lstrip()
    if stripped.startswith("%") or stripped.startswith("!"):
        return "# NOTEBOOK_MAGIC: " + stripped
    return line.rstrip("\n")


def collect_global_import_blocks_from_sources(sources: list[str]) -> list[str]:
    blocks: list[str] = []
    seen: set[str] = set()

    for source in sources:
        lines = source.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            if not (line.startswith("import ") or line.startswith("from ")):
                i += 1
                continue
            if stripped.startswith("from __future__ import"):
                i += 1
                continue

            block = [sanitize_code_line(line)]
            paren_balance = line.count("(") - line.count(")")
            while i + 1 < len(lines):
                next_line = lines[i + 1]
                next_stripped = next_line.strip()
                if paren_balance > 0:
                    block.append(sanitize_code_line(next_line))
                    paren_balance += next_line.count("(") - next_line.count(")")
                    i += 1
                    continue
                if next_line.startswith(" ") or next_line.startswith("\t"):
                    block.append(sanitize_code_line(next_line))
                    paren_balance += next_line.count("(") - next_line.count(")")
                    i += 1
                    continue
                if next_stripped.startswith(")") or next_stripped.startswith("]"):
                    block.append(sanitize_code_line(next_line))
                    paren_balance += next_line.count("(") - next_line.count(")")
                    i += 1
                    continue
                break

            block_text = "\n".join(block).strip()
            if block_text and block_text not in seen:
                seen.add(block_text)
                blocks.append(block_text)
            i += 1

    return blocks


def slugify(value: str) -> str:
    value = value.lower()
    replacements = {
        "é": "e",
        "è": "e",
        "ê": "e",
        "à": "a",
        "ù": "u",
        "ô": "o",
        "î": "i",
        "ï": "i",
        "ç": "c",
        "’": "",
        "'": "",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def normalize_pandas_udf_signatures(script_text: str) -> str:
    pattern = re.compile(
        r"(?m)^([ \t]*)def\s+([A-Za-z_][A-Za-z0-9_]*)\((.*?)\)\s*->\s*[^:]+:$"
    )

    def _replace(match: re.Match[str]) -> str:
        indent = match.group(1)
        func_name = match.group(2)
        args_part = match.group(3)
        cleaned_args = []
        for arg in args_part.split(","):
            arg = arg.strip()
            if not arg:
                continue
            cleaned_args.append(arg.split(":", 1)[0].strip())
        return f"{indent}def {func_name}({', '.join(cleaned_args)}):"

    lines = script_text.splitlines()
    normalized: list[str] = []
    previous_was_pandas_udf = False
    for line in lines:
        stripped = line.strip()
        if previous_was_pandas_udf and stripped.startswith("def "):
            line = pattern.sub(_replace, line)
        normalized.append(line)
        previous_was_pandas_udf = "@F.pandas_udf" in stripped

    return "\n".join(normalized) + "\n"


def extract_sections(cells: list[dict]) -> tuple[list[str], dict[int, dict[str, object]]]:
    prelude_code: list[str] = []
    sections: dict[int, dict[str, object]] = {}
    current_stage: int | None = None

    for cell in cells:
        if cell.get("cell_type") == "markdown":
            source = "".join(cell.get("source", []))
            match = None
            for line in source.splitlines():
                stripped = line.strip()
                match = SECTION_PATTERN.match(stripped)
                if match:
                    break
            if match:
                stage_id = int(match.group(1))
                title = match.group(2).strip()
                current_stage = stage_id
                sections.setdefault(stage_id, {"title": title, "code": []})
                continue

        if cell.get("cell_type") != "code":
            continue

        code_lines = [sanitize_code_line(line) for line in "".join(cell.get("source", [])).splitlines()]
        if current_stage is None or current_stage == 0:
            prelude_code.extend(code_lines + [""])
        else:
            sections.setdefault(current_stage, {"title": f"stage_{current_stage}", "code": []})
            sections[current_stage]["code"].extend(code_lines + [""])

    return prelude_code, sections


def render_stage_script(
    stage_id: int,
    title: str,
    shared_imports: list[str],
    prelude_code: list[str],
    stage_code: list[str],
) -> str:
    header = [
        '"""',
        f"Auto-generated stage script from padelspot.ipynb.",
        f"Stage {stage_id}: {title}",
        '"""',
        "",
        "from __future__ import annotations",
        "",
    ]
    constants_block = [
        "# Shared pipeline paths",
        *[f'{name} = "{value}"' for name, value in SHARED_PATH_CONSTANTS.items()],
        "",
    ]
    import_lines: list[str] = []
    if shared_imports:
        for block in shared_imports:
            import_lines.extend(block.splitlines())
            import_lines.append("")

    body = (
        header
        + constants_block
        + import_lines
        + prelude_code
        + [f"# ===== Stage {stage_id}: {title} =====", ""]
        + stage_code
    )
    return normalize_pandas_udf_signatures("\n".join(body).rstrip() + "\n")


def render_run_all(stage_files: list[Path]) -> str:
    imports = [
        "from __future__ import annotations",
        "",
        "import argparse",
        "import subprocess",
        "import sys",
        "from pathlib import Path",
        "",
        "",
        "def parse_args() -> argparse.Namespace:",
        "    parser = argparse.ArgumentParser(description='Run PadelSpot pipeline stages.')",
        "    parser.add_argument('--from-stage', type=int, default=1, help='First stage to run.')",
        "    parser.add_argument('--to-stage', type=int, default=7, help='Last stage to run.')",
        "    return parser.parse_args()",
        "",
        "",
        "def main() -> None:",
        "    args = parse_args()",
        "    jobs_dir = Path(__file__).resolve().parent / 'jobs'",
        "    stage_files = sorted(jobs_dir.glob('[0-9][0-9]_*.py'))",
        "    selected = []",
        "    for path in stage_files:",
        "        stage_num = int(path.name.split('_', 1)[0])",
        "        if args.from_stage <= stage_num <= args.to_stage:",
        "            selected.append((stage_num, path))",
        "    if not selected:",
        "        raise SystemExit('No stage selected.')",
        "    for stage_num, path in selected:",
        "        print(f'Running stage {stage_num}: {path.name}')",
        "        subprocess.run([sys.executable, str(path)], check=True)",
        "",
        "",
        "if __name__ == '__main__':",
        "    main()",
        "",
    ]
    return "\n".join(imports)


def main() -> None:
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    cells = notebook.get("cells", [])
    prelude_code, sections = extract_sections(cells)
    prelude_source = "\n".join(prelude_code)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    for stage_id in sorted(section_id for section_id in sections if section_id != 0):
        title = str(sections[stage_id]["title"])
        stage_code = list(sections[stage_id]["code"])
        cumulative_sources = [prelude_source]
        for previous_stage in sorted(s for s in sections if 0 < s <= stage_id):
            cumulative_sources.append("\n".join(sections[previous_stage]["code"]))
        shared_imports = collect_global_import_blocks_from_sources(cumulative_sources)
        filename = OUTPUT_DIR / f"{stage_id:02d}_{slugify(title)}.py"
        filename.write_text(
            render_stage_script(stage_id, title, shared_imports, prelude_code, stage_code),
            encoding="utf-8",
        )
        generated.append(filename)

    run_all_path = PROJECT_ROOT / "src" / "padelspot" / "main.py"
    run_all_path.write_text(render_run_all(generated), encoding="utf-8")

    print(f"Generated {len(generated)} stage scripts in: {OUTPUT_DIR}")
    print(f"Created pipeline entrypoint: {run_all_path}")


if __name__ == "__main__":
    main()
