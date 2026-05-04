# {{ project_name }}

This project was generated from the `PadelSpot` Copier template.

It includes:

- `Kedro` for project structure and orchestration
- `DVC` for data pipeline stages and artifact tracking
- `Docker + Spark` as the execution environment

## Quick start

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -e .
dvc init
kedro run
```
