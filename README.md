# PadelSpot

PadelSpot is now structured as a real data engineering project around two actual libraries from the course material:

- `Kedro` for project scaffolding and pipeline orchestration
- `DVC` for stage-level data pipeline versioning
- `Copier` for a reusable project template

`MLflow` is intentionally not part of the core scaffold because this repository does not train or register machine learning models.

## What changed

The business logic still comes from [padelspot.ipynb](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG DATA\Projet-PadelSpot\padelspot.ipynb), but the project is no longer only a notebook.

It now includes:

- a `pyproject.toml` with real `kedro` and `dvc` dependencies
- a Kedro config tree under `conf/base/`
- a Kedro pipeline registry in [src/padelspot/pipeline_registry.py](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG DATA\Projet-PadelSpot\src\padelspot\pipeline_registry.py)
- stage wrappers under `src/padelspot/pipelines/stage_*`
- DVC stages in [dvc.yaml](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG DATA\Projet-PadelSpot\dvc.yaml)
- generated execution jobs in `src/padelspot/jobs/`

## Why Kedro is useful here

Kedro gives the project a proper data engineering scaffold:

- standard project layout
- pipeline registry
- config management
- explicit pipeline names
- reproducible orchestration entrypoint

In this repository, Kedro orchestrates the existing Spark jobs through Docker, which keeps Spark execution inside the Jupyter container while still giving you a real pipeline framework.

## Why DVC is useful here

DVC is used for:

- declaring pipeline stages
- mapping dependencies to outputs
- reproducing the pipeline with tracked artifacts
- preparing dataset versioning once you initialize DVC locally

## Project structure

```text
Projet-PadelSpot/
|-- conf/
|   `-- base/
|       |-- catalog.yml
|       `-- parameters.yml
|-- data/
|-- docs/
|-- scripts/
|   |-- build_stage_scripts.py
|   |-- export_notebook_to_pipeline.py
|   `-- run_pipeline_in_docker.ps1
|-- src/
|   `-- padelspot/
|       |-- kedro_nodes.py
|       |-- pipeline_registry.py
|       |-- settings.py
|       |-- main.py
|       |-- jobs/
|       `-- pipelines/
|-- dvc.yaml
|-- docker-compose.yml
|-- pyproject.toml
`-- padelspot.ipynb
```

## Install the actual libraries

Inside your local virtual environment:

```powershell
pip install -e .
```

That installs the real Kedro and DVC packages declared in `pyproject.toml`.
Use Python `3.11` or `3.12` for this install. Avoid Python `3.13` for now, because some native dependencies in the stack may fail to build depending on the environment.

## Build stage jobs from the notebook

```powershell
python scripts/build_stage_scripts.py
```

## Run the pipeline with Kedro

Run a single pipeline:

```powershell
kedro run --pipeline stage_02_filosofi
```

Run the full pipeline:

```powershell
kedro run
```

## Run the pipeline with DVC

Rebuild the generated jobs:

```powershell
dvc repro build_stage_scripts
```

Run one tracked stage:

```powershell
dvc repro stage_05_trends
```

Run the full tracked chain:

```powershell
dvc repro
```

## What counts as a real data engineering validation

The meaningful tests are:

1. `kedro run --pipeline stage_0X_*` works for each stage
2. each stage writes its expected artifact directory
3. `kedro run` runs the full chain
4. `dvc repro` can replay the declared stages

If those four things work, the project is no longer just a notebook workflow. It has become an orchestrated and reproducible data pipeline.

## Reusable Template

This repository now also contains a real reusable template under:

- `scaffolding/copier-template/`

See:

- [docs/project_template.md](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG DATA\Projet-PadelSpot\docs\project_template.md)

That template directly addresses the "template / scaffolding tool" part of the course PDF.
Validate generated template projects with Python `3.11` or `3.12`.
