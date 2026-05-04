from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="concurrence",
        script_relative_path="src/padelspot/jobs/03_clubs_existants_res_sirene.py",
        expected_outputs=["data/output/concurrence_padel"],
        pipeline_name="stage_03_concurrence",
    )
