import hashlib
import json


def encoded(value):
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")


def item(name, content, content_type):
    return {
        "name": name,
        "content": content,
        "content_type": content_type,
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }


def upload_one(bucket, obj):
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud.storage.retry import DEFAULT_RETRY_IF_GENERATION_SPECIFIED

    blob = bucket.blob(obj["name"])
    try:
        blob.upload_from_string(
            obj["content"],
            content_type=obj["content_type"],
            if_generation_match=0,
            checksum="crc32c",
            timeout=60,
            retry=DEFAULT_RETRY_IF_GENERATION_SPECIFIED,
        )
        return "uploaded"
    except PreconditionFailed:
        blob.reload(timeout=30)
        existing = blob.download_as_bytes(
            if_generation_match=blob.generation, timeout=60
        )
        if hashlib.sha256(existing).hexdigest() != obj["sha256"]:
            raise ValueError(
                f"Existing object differs; overwrite refused: {obj['name']}"
            )
        return "already_present"
