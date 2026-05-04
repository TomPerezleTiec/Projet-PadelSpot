from __future__ import annotations

from functools import partial

from kedro.pipeline import node, pipeline

from {{ python_package }}.kedro_nodes import run_stage_in_docker


def create_stage_pipeline(
    *,
    stage_name: str,
    script_relative_path: str,
    expected_outputs: list[str],
    pipeline_name: str,
):
    return pipeline(
        [
            node(
                func=partial(
                    run_stage_in_docker,
                    stage_name=stage_name,
                    script_relative_path=script_relative_path,
                    expected_outputs=expected_outputs,
                ),
                inputs=dict(
                    compose_file="params:docker.compose_file",
                    service_name="params:docker.service_name",
                    container_root="params:docker.container_root",
                ),
                outputs=None,
                name=f"run_{pipeline_name}",
            )
        ]
    )
