from __future__ import annotations

from kedro.pipeline import Pipeline

from {{ python_package }}.pipelines.stage_01_ingestion import create_pipeline as create_stage_01
from {{ python_package }}.pipelines.stage_02_transformation import create_pipeline as create_stage_02
from {{ python_package }}.pipelines.stage_03_delivery import create_pipeline as create_stage_03


def register_pipelines() -> dict[str, Pipeline]:
    stage_01 = create_stage_01()
    stage_02 = create_stage_02()
    stage_03 = create_stage_03()
    return {
        "__default__": stage_01 + stage_02 + stage_03,
        "stage_01_ingestion": stage_01,
        "stage_02_transformation": stage_02,
        "stage_03_delivery": stage_03,
    }
