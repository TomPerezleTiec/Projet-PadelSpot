from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run PadelSpot pipeline stages.')
    parser.add_argument('--from-stage', type=int, default=1, help='First stage to run.')
    parser.add_argument('--to-stage', type=int, default=7, help='Last stage to run.')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    jobs_dir = Path(__file__).resolve().parent / 'jobs'
    stage_files = sorted(jobs_dir.glob('[0-9][0-9]_*.py'))
    selected = []
    for path in stage_files:
        stage_num = int(path.name.split('_', 1)[0])
        if args.from_stage <= stage_num <= args.to_stage:
            selected.append((stage_num, path))
    if not selected:
        raise SystemExit('No stage selected.')
    for stage_num, path in selected:
        print(f'Running stage {stage_num}: {path.name}')
        subprocess.run([sys.executable, str(path)], check=True)


if __name__ == '__main__':
    main()
