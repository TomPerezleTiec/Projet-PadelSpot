from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from padelspot.config import get_project_paths


def _is_running_inside_container() -> bool:
    return Path("/.dockerenv").exists()


def _run_command(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=str(cwd), check=True)


def ensure_docker_service(compose_file: str, service_name: str) -> None:
    paths = get_project_paths()
    if shutil.which("docker") is None:
        return
    _run_command(
        ["docker", "compose", "-f", compose_file, "up", "-d", service_name],
        cwd=paths.root,
    )


def validate_outputs(expected_outputs: Iterable[str]) -> None:
    paths = get_project_paths()
    missing = [rel for rel in expected_outputs if not (paths.root / rel).exists()]
    if missing:
        raise FileNotFoundError(
            "Stage finished but expected outputs are missing: " + ", ".join(missing)
        )


def run_stage_in_docker(
    stage_name: str,
    script_relative_path: str,
    expected_outputs: Iterable[str],
    compose_file: str = "docker-compose.yml",
    service_name: str = "jupyter",
    container_root: str = "/home/jovyan/work",
) -> str:
    """Run a generated stage script through Kedro, but inside the Spark container."""
    paths = get_project_paths()

    normalized_relative_script = script_relative_path.replace("\\", "/").lstrip("./")
    local_script_path = paths.root / normalized_relative_script
    container_script_path = f"{container_root}/{normalized_relative_script}"

    if shutil.which("docker") is None:
        if not _is_running_inside_container():
            raise RuntimeError(
                "Docker CLI is not available outside a container. Run this stage "
                "through docker compose or inside the project container."
            )
        _run_command([sys.executable, str(local_script_path)], cwd=paths.root)
    else:
        ensure_docker_service(compose_file=compose_file, service_name=service_name)
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
            cwd=paths.root,
        )
    validate_outputs(expected_outputs)
    return f"{stage_name} completed"
