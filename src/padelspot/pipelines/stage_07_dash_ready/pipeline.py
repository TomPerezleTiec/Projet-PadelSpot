from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="dash_ready",
        script_relative_path="src/padelspot/jobs/07_preparation_des_exports_dash_ready.py",
        expected_outputs=["data/dash_ready"],
        pipeline_name="stage_07_dash_ready",
    )
