from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_TOPIC = "padel_club_events"
ACTION_TO_EVENT_TYPE = {
    "create": "club_created",
    "created": "club_created",
    "update": "club_updated",
    "updated": "club_updated",
    "delete": "club_deleted",
    "deleted": "club_deleted",
}


def _default_bootstrap_servers() -> str:
    if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
        return os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if Path("/.dockerenv").exists():
        return "kafka:9092"
    return "localhost:9094"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_event(args: argparse.Namespace) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": args.event_id or f"evt_{uuid.uuid4().hex}",
        "event_type": ACTION_TO_EVENT_TYPE[args.action],
        "club_id": args.club_id,
        "event_time": args.event_time or _now_iso(),
        "source": args.source,
    }

    for field in ["name", "city", "department", "latitude", "longitude", "courts"]:
        value = getattr(args, field)
        if value is not None:
            event[field] = value

    return event


def _send_event(event: dict[str, Any], bootstrap_servers: str, topic: str) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'kafka-python'. Install project dependencies with "
            "'pip install -e .' inside the active environment."
        ) from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    metadata = producer.send(topic, key=event["club_id"], value=event).get(timeout=30)
    producer.flush()
    producer.close()
    print(
        "Sent event "
        f"{event['event_id']} to {metadata.topic}[{metadata.partition}] "
        f"offset={metadata.offset}"
    )
    print(json.dumps(event, indent=2, sort_keys=True))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send one Padel club event to Kafka.")
    parser.add_argument("action", choices=sorted(ACTION_TO_EVENT_TYPE))
    parser.add_argument("--club-id", required=True)
    parser.add_argument("--event-id")
    parser.add_argument("--event-time")
    parser.add_argument("--name")
    parser.add_argument("--city")
    parser.add_argument("--department")
    parser.add_argument("--latitude", type=float)
    parser.add_argument("--longitude", type=float)
    parser.add_argument("--courts", type=int)
    parser.add_argument("--source", default="manual_demo")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=_default_bootstrap_servers())
    return parser


def main() -> None:
    args = _parser().parse_args()
    event = _build_event(args)
    _send_event(event, args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    main()
