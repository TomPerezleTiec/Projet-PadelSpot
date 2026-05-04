from __future__ import annotations

from padelspot.pipelines.factory import create_stage_pipeline


def create_pipeline():
    return create_stage_pipeline(
        stage_name="dvf",
        script_relative_path="src/padelspot/jobs/01_donnees_dvf_demandes_de_valeurs_foncieres.py",
        expected_outputs=["data/output/dvf_clean"],
        pipeline_name="stage_01_dvf",
    )
