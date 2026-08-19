#!/usr/bin/env python3
"""fetch_batch_images.py — download a batch's rendered image variants for local review.

Why: the operator reviews composition on the platform, but a Claude Code session
cannot see the browser. This pulls every rendered variant of a batch to local
PNG files so the session can LOOK at them (Read tool renders images) and judge
composition against the rules (v870 upper two thirds, v791 close-to-camera,
hero dominance) instead of guessing from prompt text.

Usage:
  python code/fetch_batch_images.py <batch_id> [--out DIR] [--chosen-only]
                                    [--node N] [--url URL] [--token TOKEN]

Auth is the same cascade as send_to_platform.py (env KAVENO_API_TOKEN >
VEO_TOKEN > ~/veo-worker/.env > ~/.kaveno/token) — no flags needed on this box.

Output: <out>/image{scene_index:02d}_node{node_id}_var{variant_id}[_chosen].png
plus a manifest.txt listing status + prompt first-line per node.
"""

import argparse
import json
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from send_to_platform import DEFAULT_URL, resolve_token  # noqa: E402


def api_get(base, token, path):
    req = urllib.request.Request(
        base.rstrip("/") + path,
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def download(base, token, url_path, dest):
    req = urllib.request.Request(
        base.rstrip("/") + url_path,
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        data = r.read()
    with open(dest, "wb") as f:
        f.write(data)
    return len(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("batch_id")
    ap.add_argument("--out", default=None, help="output dir (default output/batch_images/<batch_id[:8]>)")
    ap.add_argument("--chosen-only", action="store_true", help="only the chosen variant per node")
    ap.add_argument("--node", type=int, default=None, help="only this scene_index_in_batch (image number)")
    ap.add_argument("--url", default=os.environ.get("KAVENO_URL", DEFAULT_URL))
    ap.add_argument("--token", default=os.environ.get("KAVENO_API_TOKEN", ""))
    args = ap.parse_args()

    token, source = resolve_token(args.token)
    if not token:
        print("no token found — set KAVENO_API_TOKEN or run send_to_platform.py set-token", file=sys.stderr)
        return 3
    out_dir = args.out or os.path.join("output", "batch_images", args.batch_id[:8])
    os.makedirs(out_dir, exist_ok=True)

    batch = api_get(args.url, token, f"/api/images/batches/{args.batch_id}")
    node_ids = batch.get("node_ids") or []
    print(f"batch {args.batch_id}: {len(node_ids)} nodes (token via {source})")

    manifest = []
    n_files = 0
    for nid in node_ids:
        node = api_get(args.url, token, f"/api/images/nodes/{nid}")
        idx = node.get("scene_index_in_batch")
        if args.node is not None and idx != args.node:
            continue
        status = node.get("status")
        chosen_id = node.get("chosen_variant_id")
        prompt_head = (node.get("prompt") or "").strip().splitlines()[0][:90] if node.get("prompt") else ""
        variants = node.get("variants") or []
        manifest.append(f"image_{idx} node={nid} status={status} variants={len(variants)} "
                        f"chosen={chosen_id} | {prompt_head}")
        for v in variants:
            vid = v.get("id")
            if args.chosen_only and chosen_id and vid != chosen_id:
                continue
            url_path = v.get("image_url")
            if not url_path:
                continue
            tag = "_chosen" if (chosen_id and vid == chosen_id) else ""
            ext = os.path.splitext((v.get("image_path") or url_path).split("?")[0])[1] or ".png"
            dest = os.path.join(out_dir, f"image{int(idx):02d}_node{nid}_var{vid}{tag}{ext}")
            try:
                size = download(args.url, token, url_path, dest)
                print(f"  saved {dest} ({size // 1024} KB)")
                n_files += 1
            except Exception as e:  # noqa: BLE001 — report and continue, a review wants the rest
                print(f"  FAILED {url_path}: {e}", file=sys.stderr)

    with open(os.path.join(out_dir, "manifest.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(manifest) + "\n")
    print(f"{n_files} file(s) in {out_dir}; manifest.txt written")
    return 0 if n_files else 1


if __name__ == "__main__":
    sys.exit(main())
