from __future__ import annotations

from {{ python_package }}.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="transformation",
        script_relative_path="src/{{ python_package }}/jobs/02_transformation.py",
        expected_outputs=["data/outputs/stage_02_transformation"],
        pipeline_name="stage_02_transformation",
    )
