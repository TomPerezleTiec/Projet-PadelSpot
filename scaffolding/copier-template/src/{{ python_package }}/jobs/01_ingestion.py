from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


OUTPUT_DIR = Path("data/outputs/stage_01_ingestion")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

payload = {
    "stage": "ingestion",
    "status": "ok",
    "generated_at": datetime.now(timezone.utc).isoformat(),
}

(OUTPUT_DIR / "metadata.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Stage 01 completed: {OUTPUT_DIR}")
