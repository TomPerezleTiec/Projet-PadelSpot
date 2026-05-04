from __future__ import annotations

from {{ python_package }}.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="delivery",
        script_relative_path="src/{{ python_package }}/jobs/03_delivery.py",
        expected_outputs=["data/outputs/stage_03_delivery"],
        pipeline_name="stage_03_delivery",
    )
