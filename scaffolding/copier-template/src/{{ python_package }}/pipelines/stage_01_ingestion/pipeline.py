from __future__ import annotations

from {{ python_package }}.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="ingestion",
        script_relative_path="src/{{ python_package }}/jobs/01_ingestion.py",
        expected_outputs=["data/outputs/stage_01_ingestion"],
        pipeline_name="stage_01_ingestion",
    )
