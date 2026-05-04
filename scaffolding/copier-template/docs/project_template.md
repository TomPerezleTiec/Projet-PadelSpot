# Project Template

This repository includes a reusable `Copier` template under `scaffolding/copier-template/`.

## Generate a new project

```powershell
pip install copier
copier copy .\scaffolding\copier-template C:\path\to\new-project
```

The generated project contains:

- Kedro configuration and pipeline registry
- DVC stages
- Docker/Spark execution wrapper
- sample jobs and outputs
