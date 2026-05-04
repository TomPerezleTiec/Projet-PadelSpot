from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="accessibilite",
        script_relative_path="src/padelspot/jobs/04_accessibilite_openstreetmap.py",
        expected_outputs=["data/output/accessibilite_clean"],
        pipeline_name="stage_04_accessibilite",
    )
