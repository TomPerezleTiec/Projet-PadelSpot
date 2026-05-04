from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


input_path = Path("data/outputs/stage_02_transformation/metadata.json")
if not input_path.exists():
    raise FileNotFoundError(f"Missing upstream artifact: {input_path}")

source = json.loads(input_path.read_text(encoding="utf-8"))
OUTPUT_DIR = Path("data/outputs/stage_03_delivery")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

payload = {
    "stage": "delivery",
    "status": "ok",
    "source_stage": source["stage"],
    "generated_at": datetime.now(timezone.utc).isoformat(),
}

(OUTPUT_DIR / "metadata.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Stage 03 completed: {OUTPUT_DIR}")
