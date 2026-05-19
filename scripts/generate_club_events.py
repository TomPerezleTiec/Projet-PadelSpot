from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timezone

from send_club_event import DEFAULT_TOPIC, _default_bootstrap_servers, _send_event


DEPARTMENTS = [
    ("59", "Lille", 50.637, 3.063),
    ("75", "Paris", 48.856, 2.352),
    ("69", "Lyon", 45.764, 4.835),
    ("31", "Toulouse", 43.604, 1.444),
    ("34", "Montpellier", 43.611, 3.877),
    ("33", "Bordeaux", 44.837, -0.579),
    ("13", "Marseille", 43.296, 5.369),
    ("44", "Nantes", 47.218, -1.553),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _event(index: int, existing_clubs: list[str]) -> dict:
    if not existing_clubs or random.random() < 0.55:
        action = "created"
    else:
        action = random.choices(["updated", "deleted"], weights=[0.75, 0.25])[0]

    department, city, lat, lon = random.choice(DEPARTMENTS)

    if action == "created":
        club_id = f"sim_club_{index:05d}"
        existing_clubs.append(club_id)
    else:
        club_id = random.choice(existing_clubs)
        if action == "deleted":
            existing_clubs.remove(club_id)

    event = {
        "event_id": f"evt_sim_{index:06d}",
        "event_type": f"club_{action}",
        "club_id": club_id,
        "event_time": _now_iso(),
        "source": "volume_simulator",
    }

    if action != "deleted":
        event.update(
            {
                "name": f"Padel Demo {club_id}",
                "city": city,
                "department": department,
                "latitude": round(lat + random.uniform(-0.05, 0.05), 6),
                "longitude": round(lon + random.uniform(-0.05, 0.05), 6),
                "courts": random.randint(2, 12),
            }
        )

    return event


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Kafka club events for demo volume.")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=_default_bootstrap_servers())
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main() -> None:
    args = _parser().parse_args()
    random.seed(args.seed)
    existing_clubs: list[str] = []

    for index in range(1, args.events + 1):
        event = _event(index, existing_clubs)
        _send_event(event, args.bootstrap_servers, args.topic)
        if args.delay > 0:
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
