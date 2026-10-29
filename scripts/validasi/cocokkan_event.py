import base64
import json
from datetime import datetime, timezone

from scripts.streaming.kontrak_event import validate


def coordinate(row):
    return row["kafka_topic"], int(row["kafka_partition"]), int(row["kafka_offset"])


def normalized(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def reconcile(raw, warehouse):
    expected = {}
    for envelope in raw:
        key = coordinate(envelope)
        event = validate(
            json.loads(
                base64.b64decode(envelope["value_base64"], validate=True).decode()
            )
        )
        if key in expected and expected[key] != event:
            raise ValueError("Conflicting raw payloads for the same Kafka coordinate")
        expected[key] = event
    found = {}
    for row in warehouse:
        key = coordinate(row)
        if key in expected:
            found.setdefault(key, []).append(row)
    missing = sorted(set(expected) - set(found))
    mismatches = []
    for key, rows in found.items():
        for row in rows:
            for field, value in expected[key].items():
                actual = normalized(row.get(field))
                if (
                    field in {"event_timestamp", "ingested_at", "replayed_at"}
                    and value is not None
                ):
                    value = (
                        datetime.fromisoformat(value.replace("Z", "+00:00"))
                        .astimezone(timezone.utc)
                        .isoformat()
                    )
                if field == "event_date" and actual is not None:
                    actual = str(actual)
                if actual != value:
                    mismatches.append({"coordinate": key, "field": field})
    return {
        "status": "passed" if expected and not missing and not mismatches else "failed",
        "raw_records": len(raw),
        "raw_coordinates": len(expected),
        "warehouse_coordinates": len(found),
        "missing": missing,
        "mismatches": mismatches,
        "duplicate_warehouse_rows": sum(len(rows) - 1 for rows in found.values()),
        "unique_event_ids": len({row["event_id"] for row in expected.values()}),
    }
