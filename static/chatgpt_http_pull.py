"""HTTP-pull mode for the ChatGPT image worker: poll the Render platform's
/api/images/worker endpoints, claim base images (backend=chatgpt routing),
download refs, generate via the drive core, upload the result, heartbeat.
Mirrors code/image_worker.py's api_pull_mode contract.

Design note: the pure helpers (make_worker_id / status_body /
job_to_prompt_and_refspec) import nothing beyond chatgpt_job_map, so the
module imports cleanly for unit tests even where `requests` is absent. The
network calls import `requests` inside the functions that use it — same lib
image_worker.py uses (import requests at api_pull_mode / _upload_variants_to_api).
The multipart field name is "files" (a list), matching image_platform.py's
worker_upload_variants(files: List[UploadFile] = File(...)).
"""
import os, sys, time, tempfile
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chatgpt_job_map as jobmap

BACKEND = "chatgpt"
API_PATH_PREFIX = "/api/images/worker"


def make_worker_id(host):
    return f"chatgpt-{host}"


def status_body(status, error=None):
    b = {"status": status}
    if error is not None:
        b["error"] = error
    return b


def job_to_prompt_and_refspec(job):
    prompt = jobmap.build_prompt(job)
    refs = [{"url": i["url"], "filename": i.get("filename") or os.path.basename(i["url"])}
            for i in sorted(job.get("input_images") or [], key=lambda i: i.get("slot_order", 0))]
    return prompt, refs


def _auth(api_key):
    return {"Authorization": f"Bearer {api_key}"}


def _download_refs(refs, api_key, dest_dir):
    import requests
    paths = []
    for r in refs:
        p = os.path.join(dest_dir, r["filename"])
        resp = requests.get(r["url"], headers=_auth(api_key), timeout=60)
        resp.raise_for_status()
        with open(p, "wb") as f:
            f.write(resp.content)
        paths.append(p)
    return paths


def run(api_url, api_key, page, host, poll_s=5, log=print):
    import requests
    from chatgpt_image_backend import generate
    wid = make_worker_id(host)
    base = api_url.rstrip("/") + API_PATH_PREFIX
    try:
        requests.post(f"{base}/release-claims", params={"worker_id": wid, "going_offline": False},
                      headers=_auth(api_key), timeout=30)
    except Exception as e:
        log(f"release-claims failed (non-fatal): {e}")
    last_hb = 0
    while True:
        now = time.time()
        if now - last_hb > 4:
            try:
                requests.post(f"{base}/heartbeat", params={"worker_id": wid},
                              headers=_auth(api_key), timeout=15)
                last_hb = now
            except Exception as e:
                log(f"heartbeat failed: {e}")
        try:
            r = requests.get(f"{base}/jobs/pending",
                             params={"worker_id": wid, "backend": BACKEND},
                             headers=_auth(api_key), timeout=30)
            job = (r.json() or {}).get("job") if r.ok else None
        except Exception as e:
            log(f"pending poll failed: {e}"); time.sleep(poll_s); continue
        if not job:
            time.sleep(poll_s); continue
        nid = job["id"]
        log(f"claimed node {nid}")
        prompt, refs = job_to_prompt_and_refspec(job)
        with tempfile.TemporaryDirectory() as td:
            try:
                ref_paths = _download_refs(refs, api_key, td)
                out_path = os.path.join(td, "variant_1.png")
                generate(page, prompt, ref_paths, out_path)
                with open(out_path, "rb") as f:
                    up = requests.post(f"{base}/jobs/{nid}/variants", headers=_auth(api_key),
                                       files=[("files", ("variant_1.png", f, "image/png"))], timeout=300)
                up.raise_for_status()
                requests.post(f"{base}/jobs/{nid}/status", headers=_auth(api_key),
                              json=status_body("completed"), timeout=30)
                log(f"  OK node {nid} uploaded")
            except Exception as e:
                try:
                    requests.post(f"{base}/jobs/{nid}/status", headers=_auth(api_key),
                                  json=status_body("failed", str(e)), timeout=30)
                except Exception:
                    pass
                log(f"  FAIL node {nid}: {e}")
        time.sleep(poll_s)
