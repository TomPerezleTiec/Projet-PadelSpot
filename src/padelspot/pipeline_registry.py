from __future__ import annotations

from kedro.pipeline import Pipeline

from padelspot.pipelines.stage_01_dvf import create_pipeline as create_stage_01
from padelspot.pipelines.stage_02_filosofi import create_pipeline as create_stage_02
from padelspot.pipelines.stage_03_concurrence import create_pipeline as create_stage_03
from padelspot.pipelines.stage_04_accessibilite import create_pipeline as create_stage_04
from padelspot.pipelines.stage_05_trends import create_pipeline as create_stage_05
from padelspot.pipelines.stage_06_score import create_pipeline as create_stage_06
from padelspot.pipelines.stage_07_dash_ready import create_pipeline as create_stage_07


def register_pipelines() -> dict[str, Pipeline]:
    stage_01 = create_stage_01()
    stage_02 = create_stage_02()
    stage_03 = create_stage_03()
    stage_04 = create_stage_04()
    stage_05 = create_stage_05()
    stage_06 = create_stage_06()
    stage_07 = create_stage_07()

    return {
        "__default__": stage_01 + stage_02 + stage_03 + stage_04 + stage_05 + stage_06 + stage_07,
        "stage_01_dvf": stage_01,
        "stage_02_filosofi": stage_02,
        "stage_03_concurrence": stage_03,
        "stage_04_accessibilite": stage_04,
        "stage_05_trends": stage_05,
        "stage_06_score": stage_06,
        "stage_07_dash_ready": stage_07,
    }

