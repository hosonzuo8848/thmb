# -*- coding: utf-8 -*-
# Generic thumbnail worker. All targets/credentials come from env (CI secrets).
# Reads a list of objects from a D1-style HTTP endpoint, fetches each source
# image from S3-compatible storage, downscales it, and writes a sibling object.
# Originals are never modified or removed; idempotent (skips if target exists).
import os, sys, io, time, argparse


def query_rows():
    import requests, urllib3
    urllib3.disable_warnings()
    token = os.environ["API_TOKEN"]
    acct  = os.environ["ACCOUNT_ID"]
    db_id = os.environ["DB_ID"]
    sql   = os.environ["SQL"]
    url = f"https://api.cloudflare.com/client/v4/accounts/{acct}/d1/database/{db_id}/query"
    r = requests.post(url, json={"sql": sql},
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                      verify=False, timeout=60)
    j = r.json()
    if not j.get("success"):
        sys.exit(f"[FATAL] query failed: {str(j.get('errors') or j)[:300]}")
    return j["result"][0].get("results", [])


def make_s3():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=os.environ["EP"],
                        aws_access_key_id=os.environ["AK"],
                        aws_secret_access_key=os.environ["SK"],
                        region_name="auto",
                        config=Config(retries={"max_attempts": 3}))


WIDTH   = int(os.environ.get("WIDTH", "400"))
QUALITY = int(os.environ.get("QUALITY", "72"))
OUT     = os.environ.get("OUT", "thumb.webp")
BKT_D   = os.environ.get("BKT_D", "")          # default bucket
BKT_A   = os.environ.get("BKT_A", "")          # alt bucket
ALT_PFX = os.environ.get("ALT_PFX", "")        # prefix that maps to alt bucket


def bucket_for(prefix):
    return BKT_A if (ALT_PFX and (prefix or "").startswith(ALT_PFX)) else BKT_D


def src_key(prefix, page_count, page):
    p = 0
    try:
        p = int(page or 0)
    except (TypeError, ValueError):
        p = 0
    if p < 1 or (page_count and p > page_count):
        p = 1
    return f"{prefix}page_{p:04d}.webp"


def head_size(s3, bucket, key):
    from botocore.exceptions import ClientError
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def downscale(data):
    from PIL import Image
    im = Image.open(io.BytesIO(data)); im.load()
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")
    w, h = im.size
    if w > WIDTH:
        im = im.resize((WIDTH, max(1, round(h * WIDTH / w))), Image.LANCZOS)
    out = io.BytesIO()
    im.save(out, format="WEBP", quality=QUALITY, method=6)
    return out.getvalue()


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--total", type=int, default=1)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    s3 = make_s3()
    rows = query_rows()
    if args.total > 1:
        rows = rows[args.shard::args.total]
    n = len(rows)
    print(f"[start] shard {args.shard}/{args.total} rows={n} width={WIDTH} q={QUALITY}", flush=True)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    lock = threading.Lock()
    st = {"made": 0, "skip": 0, "miss": 0, "err": 0, "ob": 0, "tb": 0}
    t0 = time.time()

    def one(b):
        prefix = b["webp_prefix"]
        bucket = bucket_for(prefix)
        tkey = f"{prefix}{OUT}"
        if not args.force and head_size(s3, bucket, tkey) is not None:
            return ("skip", 0, 0)
        sk = src_key(prefix, b.get("page_count"), b.get("cover_page"))
        try:
            data = s3.get_object(Bucket=bucket, Key=sk)["Body"].read()
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            return ("miss" if code in ("NoSuchKey", "404", "NotFound") else "err", 0, 0)
        try:
            th = downscale(data)
            s3.put_object(Bucket=bucket, Key=tkey, Body=th, ContentType="image/webp")
            head_size(s3, bucket, tkey)
        except Exception as e:
            print(f"  ERR {tkey}: {str(e)[:80]}", flush=True)
            return ("err", 0, 0)
        return ("made", len(data), len(th))

    done = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        for fut in as_completed([ex.submit(one, b) for b in rows]):
            status, ob, tb = fut.result()
            with lock:
                done += 1
                st[status] += 1
                st["ob"] += ob; st["tb"] += tb
                if done % 200 == 0 or done == n:
                    print(f"  [{done}/{n}] made={st['made']} skip={st['skip']} "
                          f"miss={st['miss']} err={st['err']} "
                          f"{done/max(0.1, time.time()-t0):.1f}/s", flush=True)

    ratio = (st["ob"] / st["tb"]) if st["tb"] else 0
    print(f"[done] {st} avg_ratio={ratio:.1f}x elapsed={time.time()-t0:.0f}s", flush=True)
    sys.exit(1 if st["err"] else 0)


if __name__ == "__main__":
    main()
