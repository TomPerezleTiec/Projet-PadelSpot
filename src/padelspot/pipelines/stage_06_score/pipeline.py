from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="score",
        script_relative_path="src/padelspot/jobs/06_score_composite_dimplantation.py",
        expected_outputs=["data/output/score_final_full"],
        pipeline_name="stage_06_score",
    )
