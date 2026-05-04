# Project Template

This repository now includes a reusable `Copier` template under:

- `scaffolding/copier-template/`

## Why this matters

The course PDF explicitly mentions:

- scaffolding
- templates
- project standardization

This template makes that concrete with a reusable base built around:

- `Kedro`
- `DVC`
- `Docker + Spark`

## How to generate a new project

Install Copier:

```powershell
pip install copier
```

Generate a new project:

```powershell
copier copy .\scaffolding\copier-template C:\path\to\new-project
```

## What the generated project contains

- `pyproject.toml`
- `conf/base/`
- `src/<package>/pipeline_registry.py`
- `src/<package>/kedro_nodes.py`
- `src/<package>/pipelines/`
- `src/<package>/jobs/`
- `dvc.yaml`
- `docker-compose.yml`

## How to validate the generated project

Inside the generated project:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -e .
dvc init
kedro run
dvc repro
```

If those steps work, the template is a valid reusable data engineering scaffold.

Important:

- use Python `3.11` or `3.12`
- avoid Python `3.13` for now, because native dependencies may fail to build in some environments
