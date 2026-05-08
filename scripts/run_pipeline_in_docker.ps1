param(
    [switch]$ExportOnly,
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ExportScript = Join-Path $ProjectRoot "scripts\export_notebook_to_pipeline.py"
$GeneratedPipelineWindows = Join-Path $ProjectRoot "src\padelspot\pipelines\padelspot_pipeline_from_notebook.py"
$GeneratedPipelineDocker = "/home/jovyan/work/src/padelspot/pipelines/padelspot_pipeline_from_notebook.py"
$RequirementsDocker = Join-Path $ProjectRoot "requirements-docker.txt"
$RequirementsDockerInContainer = "/home/jovyan/work/requirements-docker.txt"

if (Test-Path (Join-Path $ProjectRoot ".venv\Scripts\python.exe")) {
    $PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
} else {
    $PythonExe = "python"
}

Write-Host "Export du notebook vers un script Python..."
& $PythonExe $ExportScript --include-markdown

if (-not (Test-Path $GeneratedPipelineWindows)) {
    throw "Le script exporte n'a pas ete genere : $GeneratedPipelineWindows"
}

if ($ExportOnly) {
    Write-Host "Export termine. Execution Docker ignoree."
    exit 0
}

Write-Host "Demarrage du conteneur Jupyter/Spark..."
docker compose up -d jupyter

Write-Host "Verification des dependances Python dans le conteneur..."
$HasPySpark = $true
try {
    docker compose exec jupyter /opt/conda/bin/python -c "import importlib.util; raise SystemExit(0 if importlib.util.find_spec('pyspark') else 1)"
} catch {
    $HasPySpark = $false
}

if (-not $HasPySpark -and -not $InstallDeps) {
    Write-Host ""
    Write-Host "Le conteneur Docker ne contient pas encore 'pyspark'." -ForegroundColor Yellow
    Write-Host "Installe les dependances une premiere fois avec :" -ForegroundColor Yellow
    Write-Host "powershell -ExecutionPolicy Bypass -File .\scripts\run_pipeline_in_docker.ps1 -InstallDeps" -ForegroundColor Cyan
    Write-Host ""
    throw "Dependance manquante dans le conteneur : pyspark"
}

if (-not $HasPySpark -and $InstallDeps) {
    if (-not (Test-Path $RequirementsDocker)) {
        throw "Fichier requirements introuvable : $RequirementsDocker"
    }

    Write-Host "Installation des dependances Python dans le conteneur..."
    docker compose exec jupyter /opt/conda/bin/pip install -r $RequirementsDockerInContainer
}

Write-Host "Execution du pipeline dans Docker..."
docker compose exec jupyter /home/jovyan/New-Projet-PadelSpot-v2/.venv/bin/python $GeneratedPipelineDocker
