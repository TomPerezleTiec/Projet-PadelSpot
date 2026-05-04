# Data Engineering Alignment

This project now uses real libraries from the course material instead of only imitating their structure.

## Libraries used

### Kedro

Used for:

- scaffolding the project structure
- registering named pipelines
- centralizing configuration under `conf/base/`
- orchestrating the data pipeline from a proper framework entrypoint

Why it fits here:

- the project already had multiple business stages
- those stages needed a real orchestrator
- Spark execution stays inside Docker, while Kedro manages the pipeline from the host project

### DVC

Used for:

- explicit stage declaration in `dvc.yaml`
- dependency/output tracking
- reproducible pipeline replay
- preparation for data versioning beyond Git

Why it fits here:

- the repository writes large intermediate artifacts under `data/output/`
- Git is not appropriate for versioning those outputs
- the course explicitly calls out data and pipeline versioning

## Library intentionally not prioritized

### MLflow

MLflow is not central here because this repository does not train, compare, register, or deploy models.

If the project later evolves toward:

- predictive scoring
- XGBoost model training
- model comparison
- experiment tracking

then MLflow becomes relevant.

For the current scope, Kedro + DVC is the correct data engineering core.

## What was added

### Kedro scaffold

- `pyproject.toml`
- `conf/base/catalog.yml`
- `conf/base/parameters.yml`
- `src/padelspot/settings.py`
- `src/padelspot/pipeline_registry.py`
- `src/padelspot/pipelines/stage_*`
- `src/padelspot/kedro_nodes.py`

### DVC scaffold

- `dvc.yaml` with one stage per pipeline step

## Execution model

The project uses a hybrid model:

1. the notebook remains the source of business logic
2. `scripts/build_stage_scripts.py` extracts stage scripts
3. Kedro orchestrates those stage scripts
4. each stage runs inside the Docker Spark container
5. DVC tracks the resulting data artifacts

This is a valid data engineering setup because it gives:

- orchestration
- reproducibility
- explicit outputs
- framework-backed project structure
- a path toward industrialization

## What still remains for a stronger v2

The next improvements would be:

- replacing generated stage scripts with hand-maintained modules
- pushing more logic from notebook cells into reusable Python modules
- using Kedro datasets more deeply as direct node inputs/outputs instead of stage wrappers
- initializing DVC remote storage if the project needs collaborative artifact sharing

Even without those improvements, the repository now uses actual data engineering libraries from the course in a meaningful way.
