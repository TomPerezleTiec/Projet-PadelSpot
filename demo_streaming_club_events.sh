#!/bin/bash

set -e

PYTHON_BIN="${PYTHON_BIN:-/opt/conda/bin/python}"

echo "==================================================="
echo "   DEMO: PadelSpot Kafka Streaming Club Events"
echo "==================================================="
echo ""

echo "1. Environment"
echo "---"
pwd
echo "Python binary: ${PYTHON_BIN}"
"${PYTHON_BIN}" --version
echo "Kafka bootstrap: ${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
echo ""

echo "2. Send demo events to Kafka"
echo "---"
"${PYTHON_BIN}" scripts/send_club_event.py create --club-id club_demo_001 --name "Padel Arena Lille" --city Lille --department 59 --latitude 50.637 --longitude 3.063 --courts 6
"${PYTHON_BIN}" scripts/send_club_event.py update --club-id club_demo_001 --name "Padel Arena Lille" --city Lille --department 59 --latitude 50.637 --longitude 3.063 --courts 8
"${PYTHON_BIN}" scripts/send_club_event.py create --club-id club_demo_002 --name "Padel Demo Montpellier" --city Montpellier --department 34 --latitude 43.611 --longitude 3.877 --courts 4
echo ""

echo "3. Generate volume"
echo "---"
"${PYTHON_BIN}" scripts/generate_club_events.py --events 50 --delay 0
echo ""

echo "4. Consume Kafka topic with Spark Structured Streaming"
echo "---"
"${PYTHON_BIN}" src/padelspot/streaming/club_events_stream.py --once
echo ""

echo "5. Output tables"
echo "---"
du -sh data/streaming/* 2>/dev/null || true
echo ""

echo "Bronze sample:"
"${PYTHON_BIN}" - <<'PY'
from pathlib import Path
from pyspark.sql import SparkSession

root = Path("/home/jovyan/work") if Path("/home/jovyan/work").exists() else Path.cwd()
spark = SparkSession.builder.appName("padelspot-streaming-demo-preview").getOrCreate()
for name in ["bronze_club_events", "silver_clubs_current", "gold_clubs_by_department"]:
    path = root / "data" / "streaming" / name
    print(f"\n{name}: {path}")
    if path.exists():
        spark.read.parquet(str(path)).show(10, truncate=False)
    else:
        print("missing")
spark.stop()
PY

echo ""
echo "Streaming demo completed."
