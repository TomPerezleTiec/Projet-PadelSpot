from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="filosofi",
        script_relative_path="src/padelspot/jobs/02_donnees_filosofi_insee_2021.py",
        expected_outputs=["data/output/filosofi_clean"],
        pipeline_name="stage_02_filosofi",
    )
