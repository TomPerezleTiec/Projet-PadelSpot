from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="trends",
        script_relative_path="src/padelspot/jobs/05_demande_latente_google_trends.py",
        expected_outputs=["data/output/trends_joined"],
        pipeline_name="stage_05_trends",
    )
