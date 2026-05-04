from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterable


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_command(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=str(cwd), check=True)


def ensure_docker_service(compose_file: str, service_name: str) -> None:
    _run_command(
        ["docker", "compose", "-f", compose_file, "up", "-d", service_name],
        cwd=_project_root(),
    )


def validate_outputs(expected_outputs: Iterable[str]) -> None:
    root = _project_root()
    missing = [rel for rel in expected_outputs if not (root / rel).exists()]
    if missing:
        raise FileNotFoundError("Missing expected outputs: " + ", ".join(missing))


def run_stage_in_docker(
    *,
    stage_name: str,
    script_relative_path: str,
    expected_outputs: Iterable[str],
    compose_file: str,
    service_name: str,
    container_root: str,
) -> str:
    ensure_docker_service(compose_file=compose_file, service_name=service_name)
    normalized_relative_script = script_relative_path.replace("\\", "/").lstrip("./")
    container_script_path = f"{container_root}/{normalized_relative_script}"
    _run_command(
        [
            "docker",
            "compose",
            "-f",
            compose_file,
            "exec",
            service_name,
            "python",
            container_script_path,
        ],
        cwd=_project_root(),
    )
    validate_outputs(expected_outputs)
    return f"{stage_name} completed"
