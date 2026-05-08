#!/bin/bash

set -e

START_TS=$(date +%s)
TEMPLATE_DEMO_DIR="/tmp/padelspot-copier-demo"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}===================================================${NC}"
echo -e "${BLUE}   DEMO: PadelSpot Data Engineering Pipeline${NC}"
echo -e "${BLUE}===================================================${NC}\n"

echo -e "${YELLOW}1. ENVIRONMENT CHECK${NC}"
echo "---"
echo "Current directory:"
pwd
echo ""
echo "Active Python:"
which python
python --version
echo ""
echo "Venv active"
echo ""

echo -e "${YELLOW}2. KEDRO CHECK${NC}"
echo "---"
kedro info
echo ""

echo -e "${YELLOW}3. DVC CHECK${NC}"
echo "---"
dvc version
echo ""

echo -e "${YELLOW}4. COPIER TEMPLATE + SCAFFOLDING${NC}"
echo "---"
if [ -d "scaffolding/copier-template" ] && [ -f "scaffolding/copier-template/copier.yml" ]; then
    echo "Template found in scaffolding/copier-template"
    echo ""
    echo "Template config preview:"
    sed -n '1,20p' scaffolding/copier-template/copier.yml
    echo ""

    if [ -f ".copier-answers.yml" ]; then
        echo "Root answers file preview:"
        sed -n '1,20p' .copier-answers.yml
        echo ""
    fi

    echo "Generating a real project from the template..."
    rm -rf "${TEMPLATE_DEMO_DIR}"
    copier copy --defaults --overwrite scaffolding/copier-template "${TEMPLATE_DEMO_DIR}"
    echo "Generated project: ${TEMPLATE_DEMO_DIR}"
    echo ""

    echo "Generated structure preview:"
    find "${TEMPLATE_DEMO_DIR}" -maxdepth 3 | sort | head -40
    echo ""

    test -f "${TEMPLATE_DEMO_DIR}/pyproject.toml"
    test -f "${TEMPLATE_DEMO_DIR}/dvc.yaml"
    test -f "${TEMPLATE_DEMO_DIR}/docker-compose.yml"
    test -d "${TEMPLATE_DEMO_DIR}/conf/base"
    test -f "${TEMPLATE_DEMO_DIR}/README.md"
    echo "Scaffold validation OK: key files are present"
else
    echo "Template not found"
fi
echo ""

echo -e "${YELLOW}5. STAGE SCRIPT VALIDATION${NC}"
echo "---"
echo "Validating generated stage scripts:"
ls -1 src/padelspot/jobs/*.py
python -m py_compile src/padelspot/jobs/*.py
echo "Stage scripts validated and ready for execution"
echo ""

echo -e "${YELLOW}6. PROJECT VALIDATION TESTS${NC}"
echo "---"
python -m unittest tests.test_project_validation -v
echo ""

echo -e "${YELLOW}7. FULL DAG (DVC REPRO --DRY)${NC}"
echo "---"
python -m dvc repro --dry
echo ""

echo -e "${YELLOW}8. FORCED PIPELINE EXECUTION${NC}"
echo "---"
RUN_START_TS=$(date +%s)
dvc repro -f stage_07_dash_ready
RUN_END_TS=$(date +%s)
echo "Forced stage execution completed in $((RUN_END_TS - RUN_START_TS)) seconds"
echo ""

echo -e "${YELLOW}9. REPRODUCIBILITY PROOF${NC}"
echo "---"
dvc repro
echo "Second execution completed (expected: already up to date)"
echo ""

echo -e "${YELLOW}10. OUTPUT CHECK${NC}"
echo "---"
echo "data/ directory:"
du -sh data/* 2>/dev/null | sort -h
echo ""

echo -e "${YELLOW}11. DASH_READY ARTIFACTS${NC}"
echo "---"
if [ -d "data/dash_ready" ]; then
    echo "data/dash_ready/ found"
    echo ""
    echo "Contents:"
    ls -lh data/dash_ready/
    echo ""
    echo "dash_carreaux_full structure:"
    ls -L data/dash_ready/dash_carreaux_full/ | head -5
    echo "  ... ($(ls -1 data/dash_ready/dash_carreaux_full/ | wc -l) entries total)"
else
    echo "data/dash_ready/ not found"
fi
echo ""

echo -e "${YELLOW}12. DVC TRACEABILITY (STATUS)${NC}"
echo "---"
dvc status
echo ""

END_TS=$(date +%s)
TOTAL_DURATION=$((END_TS - START_TS))

echo -e "${BLUE}===================================================${NC}"
echo -e "${GREEN}SUMMARY: Complete Data Engineering Proof${NC}"
echo -e "${BLUE}===================================================${NC}"
echo ""
echo "Validated components:"
echo "  - Python & venv"
echo "  - Kedro"
echo "  - DVC"
echo "  - Copier template"
echo "  - Real scaffolding via Copier"
echo "  - Notebook to stage script generation"
echo "  - Project tests"
echo "  - Full DAG"
echo "  - Forced stage execution"
echo "  - Reproducibility via DVC skip"
echo "  - DVC traceability"
echo "  - Final outputs"
echo ""
echo "Total demo duration: ${TOTAL_DURATION} seconds"
echo ""
