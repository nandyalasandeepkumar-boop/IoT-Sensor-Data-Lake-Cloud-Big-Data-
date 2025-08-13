import json, os, uuid
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]

def _partitioned_key(evt: dict) -> str:
    # ensure timestamp
    ts = evt.get("ts")
    if ts:
        # allow both epoch seconds and ISO strings
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except Exception:
            dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
    else:
        dt = datetime.now(tz=timezone.utc)

    device_id = str(evt.get("device_id", "unknown"))
    y = dt.strftime("%Y")
    m = dt.strftime("%m")
    d = dt.strftime("%d")
    h = dt.strftime("%H")
    uid = uuid.uuid4().hex
    return f"raw/year={y}/month={m}/day={d}/hour={h}/device_id={device_id}/event_{uid}.json"

def handler(event, context):
    # Lambda Function URL → event['body'] contains JSON string
    if "body" not in event:
        return {"statusCode": 400, "body": "Missing body"}

    try:
        body = event["body"]
        if isinstance(body, str):
            payload = json.loads(body)
        else:
            payload = body
    except Exception as e:
        return {"statusCode": 400, "body": f"Invalid JSON: {e}"}

    # basic validation
    for k in ["device_id", "temperature", "humidity"]:
        if k not in payload:
            return {"statusCode": 400, "body": f"Missing '{k}'"}

    # add server timestamp if absent
    if "ts" not in payload:
        payload["ts"] = datetime.now(tz=timezone.utc).isoformat()

    key = _partitioned_key(payload)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )

    return {"statusCode": 200, "body": json.dumps({"ok": True, "s3_key": key})}
