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

WORKER_BACKEND = "chatgpt"
# Backwards-compat alias (older callers referenced BACKEND).
BACKEND = WORKER_BACKEND
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


def _post_retry(url, tries=4, timeout=300, log=print, **kw):
    """POST with retry on TRANSIENT failures — network drops (Connection aborted /
    RemoteDisconnected), timeouts, and 5xx. A 4xx is NOT retried (client error).
    The caller must pass file bytes (not an open handle) so the body can be resent.
    Raises the last error if all attempts fail."""
    import time as _t
    import requests
    from requests.exceptions import (ConnectionError as _CE, Timeout as _TO,
                                     ChunkedEncodingError as _CEE, HTTPError as _HE)
    last = None
    for i in range(tries):
        try:
            r = requests.post(url, timeout=timeout, **kw)
            r.raise_for_status()
            return r
        except _HE as e:
            code = getattr(getattr(e, "response", None), "status_code", 0) or 0
            if not (500 <= code < 600):
                raise                    # 4xx — don't retry
            last = e
        except (_CE, _TO, _CEE) as e:
            last = e
        if i < tries - 1:
            wait = min(2 ** i, 15)
            log(f"  upload transient fail ({type(last).__name__}); retry "
                f"{i + 1}/{tries - 1} in {wait}s")
            _t.sleep(wait)
    raise last


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
                             params={"worker_id": wid, "backend": WORKER_BACKEND},
                             headers=_auth(api_key), timeout=30)
            job = (r.json() or {}).get("job") if r.ok else None
        except Exception as e:
            log(f"pending poll failed: {e}"); time.sleep(poll_s); continue
        if not job:
            time.sleep(poll_s); continue
        nid = job["id"]
        nname = job.get("name") or ""
        prompt, refs = job_to_prompt_and_refspec(job)
        # Match the image worker's claim line so the operator can tell WHICH node
        # (and what) was claimed: name + ref count + prompt preview.
        preview = " ".join((prompt or "").split())[:70]
        log(f"-> claimed node {nid}" + (f" ({nname})" if nname else "")
            + (f" | {len(refs)} ref(s)" if refs else "")
            + (f' | "{preview}..."' if preview else ""))
        with tempfile.TemporaryDirectory() as td:
            try:
                ref_paths = _download_refs(refs, api_key, td)
                out_path = os.path.join(td, "variant_1.png")
                generate(page, prompt, ref_paths, out_path)
                # Read bytes once so the upload body can be resent on retry — a
                # dropped connection (RemoteDisconnected) used to fail the node
                # even though the image was ready.
                with open(out_path, "rb") as f:
                    data = f.read()
                up = _post_retry(f"{base}/jobs/{nid}/variants", headers=_auth(api_key),
                                 params={"backend": WORKER_BACKEND},
                                 files=[("files", ("variant_1.png", data, "image/png"))],
                                 timeout=300, log=log)
                st = _post_retry(f"{base}/jobs/{nid}/status", headers=_auth(api_key),
                                 params={"backend": WORKER_BACKEND},
                                 json=status_body("completed"), timeout=30, tries=3, log=log)
                # Surface the SERVER's verdict so a dropped/superseded chatgpt
                # variant is visible in the worker log (not a silent loss).
                try:
                    uj = up.json() if up is not None else {}
                    sj = st.json() if st is not None else {}
                except Exception:
                    uj, sj = {}, {}
                saved = uj.get("saved_count")
                sup = uj.get("superseded")
                cg = sj.get("cg_status")
                tail = f" [saved={saved} superseded={sup} cg_status={cg}]"
                if sup or (saved == 0) or (cg == "failed"):
                    log(f"  ⚠ node {nid}" + (f" ({nname})" if nname else "")
                        + f" uploaded but server DROPPED it{tail}")
                else:
                    log(f"  OK node {nid}" + (f" ({nname})" if nname else "")
                        + f" uploaded{tail}")
            except Exception as e:
                try:
                    requests.post(f"{base}/jobs/{nid}/status", headers=_auth(api_key),
                                  params={"backend": WORKER_BACKEND},
                                  json=status_body("failed", str(e)), timeout=30)
                except Exception:
                    pass
                log(f"  FAIL node {nid}" + (f" ({nname})" if nname else "") + f": {e}")
        time.sleep(poll_s)
