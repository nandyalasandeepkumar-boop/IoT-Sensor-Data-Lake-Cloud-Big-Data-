import os, time, json, random, argparse
from datetime import datetime, timezone
import requests

def generate_event(device_id: str):
    # Simple bounded random walk for realism
    base_t = 22.0 + random.uniform(-2, 2)
    base_h = 45.0 + random.uniform(-5, 5)
    return {
        "device_id": device_id,
        "temperature": round(base_t + random.uniform(-0.8, 0.8), 2),
        "humidity": round(base_h + random.uniform(-3, 3), 2),
        "ts": datetime.now(tz=timezone.utc).isoformat()
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-url", default=os.getenv("INGEST_URL"), help="Lambda Function URL")
    ap.add_argument("--device-count", type=int, default=3)
    ap.add_argument("--rate-per-sec", type=float, default=1.0, help="events per device per second")
    ap.add_argument("--duration-sec", type=int, default=30)
    args = ap.parse_args()

    if not args.ingest_url:
        raise SystemExit("Missing --ingest-url or INGEST_URL env var")

    devices = [f"dev-{i+1:03d}" for i in range(args.device_count)]
    interval = 1.0 / max(args.rate_per_sec, 0.001)
    end_time = time.time() + args.duration_sec

    sent = 0
    while time.time() < end_time:
        for d in devices:
            evt = generate_event(d)
            r = requests.post(args.ingest_url, json=evt, timeout=5)
            if r.status_code >= 300:
                print("POST failed:", r.status_code, r.text)
            else:
                sent += 1
        time.sleep(interval)

    print(f"Done. Sent {sent} events.")

if __name__ == "__main__":
    main()
