"""Shared S3 state for the transformed batch awaiting warehouse loading."""

import json

import boto3


BUCKET = "agri-data-engineering"
MANIFEST_KEY = "checkpoints/air_quality_transform_manifest.json"


def _client():
    return boto3.client("s3", region_name="eu-north-1")


def read_manifest():
    s3 = _client()
    try:
        response = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
    except s3.exceptions.NoSuchKey:
        return {"objects": {}, "pending_batch": None}

    manifest = json.loads(response["Body"].read().decode("utf-8"))
    if "objects" not in manifest:
        manifest = {"objects": manifest, "pending_batch": None}
    return manifest


def get_pending_batch():
    return read_manifest().get("pending_batch")


def complete_pending_batch(batch_id):
    """Clear a batch only after its warehouse transaction succeeds."""
    manifest = read_manifest()
    pending = manifest.get("pending_batch")
    if pending is None:
        return
    if pending.get("id") != batch_id:
        raise RuntimeError(
            "Pending transformed batch changed before completion."
        )

    manifest["pending_batch"] = None
    _client().put_object(
        Bucket=BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
