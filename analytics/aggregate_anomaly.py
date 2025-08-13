import argparse, json, io
from datetime import datetime
import pandas as pd
import numpy as np
import boto3
import s3fs
from sklearn.ensemble import IsolationForest

def list_objects(bucket, prefix):
    s3 = boto3.client("s3")
    token = None
    keys = []
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
        if token: kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for c in resp.get("Contents", []):
            if c["Key"].endswith(".json"):
                keys.append(c["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def read_json_records(bucket, keys):
    fs = s3fs.S3FileSystem()
    records = []
    for k in keys:
        with fs.open(f"s3://{bucket}/{k}", "rb") as f:
            rec = json.load(f)
            records.append(rec)
    return pd.DataFrame.from_records(records)

def hourly_agg(df):
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["hour"] = df["ts"].dt.floor("H")
    grp = df.groupby(["device_id","hour"]).agg(
        temp_avg=("temperature","mean"),
        temp_min=("temperature","min"),
        temp_max=("temperature","max"),
        hum_avg=("humidity","mean"),
        count=("temperature","count")
    ).reset_index()
    return grp

def detect_anomalies(grp):
    # IsolationForest per device on temp_avg and hum_avg
    out_frames = []
    for dev, g in grp.groupby("device_id"):
        feats = g[["temp_avg","hum_avg"]].values
        if len(g) < 10:
            g["anomaly"] = False
        else:
            clf = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
            preds = clf.fit_predict(feats)
            g["anomaly"] = preds == -1
        out_frames.append(g)
    return pd.concat(out_frames, ignore_index=True)

def write_parquet(df, s3_uri):
    df["date"] = df["hour"].dt.date.astype(str)
    # write a single Parquet file per run (simple demo)
    table_uri = s3_uri.rstrip("/")
    fs = s3fs.S3FileSystem()
    # derive path
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = f"{table_uri}/run_ts={ts}/aggregates.parquet"
    with fs.open(path, "wb") as f:
        df.to_parquet(f, index=False)
    print("Wrote:", path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="raw/")
    ap.add_argument("--output", required=True, help="s3://bucket/curated")
    args = ap.parse_args()

    keys = list_objects(args.bucket, args.prefix)
    if not keys:
        raise SystemExit("No data found under prefix.")
    df = read_json_records(args.bucket, keys)
    grp = hourly_agg(df)
    res = detect_anomalies(grp)
    write_parquet(res, args.output)
