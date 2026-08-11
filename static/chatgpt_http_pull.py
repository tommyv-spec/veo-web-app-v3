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


# Playwright's wording when the browser/context/page is gone. A dead browser is
# NOT a bad node: marking the node failed here is how one crash burned a whole
# queue (2026-08-11 — the browser died after a node completed and every later
# claim failed instantly with "Target page, context or browser has been closed",
# destroying nodes 4783/4801/... in seconds).
_BROWSER_DEAD_MARKERS = (
    "target page, context or browser has been closed",
    "browser has been closed",
    "target closed",
    "browser closed",
    "connection closed while reading from the driver",
)


def _browser_dead(exc):
    return any(m in str(exc).lower() for m in _BROWSER_DEAD_MARKERS)


def _page_alive(page):
    """True if this page can still be driven."""
    try:
        return page is not None and not page.is_closed()
    except Exception:
        return False


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


def run(api_url, api_key, page, host, poll_s=5, log=print, relaunch=None):
    import requests
    from chatgpt_image_backend import generate
    wid = make_worker_id(host)
    base = api_url.rstrip("/") + API_PATH_PREFIX
    try:
        requests.post(f"{base}/release-claims", params={"worker_id": wid, "going_offline": False},
                      headers=_auth(api_key), timeout=30)
    except Exception as e:
        log(f"release-claims failed (non-fatal): {e}")

    # v897 — heartbeat from a DAEMON THREAD, not from the poll loop.
    #
    # The beat used to be sent inline right before generate(), which blocks for
    # minutes while the browser renders. No request goes out for that whole
    # span, so the server's heartbeat row ages past its 20s window and the
    # platform light flips to "offline" while the worker is demonstrably
    # working (operator caught this 2026-08-05: "the chatgpt worker is live").
    # flow_worker already beats from a thread; this brings parity.
    #
    # Server-side v897 also treats a fresh claim as liveness, so an OLD copy of
    # this worker still reads correctly — but a real beat is the honest signal.
    import threading
    _hb_stop = threading.Event()

    def _heartbeat_loop():
        while not _hb_stop.is_set():
            try:
                requests.post(f"{base}/heartbeat", params={"worker_id": wid},
                              headers=_auth(api_key), timeout=15)
            except Exception as e:
                log(f"heartbeat failed: {e}")
            _hb_stop.wait(4.0)

    _hb_thread = threading.Thread(target=_heartbeat_loop, name="chatgpt-heartbeat", daemon=True)
    _hb_thread.start()

    # v898 — GRACEFUL SHUTDOWN. Closing the worker used to leave the platform
    # showing "● Online": nothing ever called release-claims with
    # going_offline=true, so the heartbeat row survived AND the in-flight node
    # stayed cg_status=generating with a fresh cg_claimed_at, which the server's
    # v897 busy-liveness rule reads as proof the worker is alive (operator
    # 2026-08-03: "i closed the worker but in the platform it still shows it's
    # online"). The server endpoint has deleted the heartbeat row on
    # going_offline=true since v516 — this worker simply never called it.
    #
    # Releasing the claim also puts the node it was mid-way through back in the
    # queue instead of stranding it in "generating" until the 10-minute sweep.
    _shutdown_done = threading.Event()

    def _go_offline(reason=""):
        if _shutdown_done.is_set():
            return
        _shutdown_done.set()
        _hb_stop.set()                      # stop beating BEFORE clearing the row
        try:
            r = requests.post(f"{base}/release-claims",
                              params={"worker_id": wid, "going_offline": True},
                              headers=_auth(api_key), timeout=15)
            j = r.json() if r.ok else {}
            log(f"going offline{f' ({reason})' if reason else ''} — "
                f"released {j.get('released', '?')} claim(s), "
                f"heartbeat_deleted={j.get('heartbeat_deleted', '?')}")
        except Exception as e:
            log(f"going offline{f' ({reason})' if reason else ''} — "
                f"release-claims failed, the platform light will go red on its "
                f"own within the heartbeat window: {e}")

    import atexit
    import signal
    atexit.register(_go_offline, "process exit")
    for _sig in ("SIGINT", "SIGTERM", "SIGBREAK"):
        s = getattr(signal, _sig, None)
        if s is None:
            continue                        # SIGBREAK is Windows-only
        try:
            signal.signal(s, lambda _s, _f: (_go_offline(_sig), sys.exit(0)))
        except (ValueError, OSError):
            pass                            # not on the main thread — atexit covers it

    try:
        _pump(base, api_key, wid, page, poll_s, log, relaunch=relaunch)
    finally:
        _go_offline("worker loop ended")


def _pump(base, api_key, wid, page, poll_s, log, relaunch=None):
    """The claim -> generate -> upload loop. Split out so `run` can wrap it in
    the graceful-shutdown try/finally without re-indenting the whole body.

    `relaunch()` returns a fresh logged-in page (or None). Without it a dead
    browser is terminal and the worker can only stop."""
    import requests
    from chatgpt_image_backend import generate

    dead_since = 0          # consecutive relaunch failures, for the backoff

    while True:
        # Never claim a node we cannot possibly render. Claiming against a dead
        # browser is what turned one crash into a queue-wide wipe.
        if not _page_alive(page):
            if not relaunch:
                log("browser is gone and no relaunch is available — stopping so "
                    "the queue is not burned")
                return
            dead_since += 1
            wait = min(15 * dead_since, 120)
            log(f"browser is gone — relaunching (attempt {dead_since})")
            try:
                page = relaunch()
            except Exception as e:
                page = None
                log(f"  relaunch raised: {str(e).splitlines()[0][:120]}")
            if not _page_alive(page):
                log(f"  relaunch failed — retrying in {wait}s")
                time.sleep(wait)
                continue
            log("  browser back up")
            dead_since = 0
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
                if _browser_dead(e):
                    # The NODE is fine; the browser died under it. Leave the
                    # claim alone — the server's stale-claim sweep requeues it —
                    # and never write status=failed, which is unrecoverable and
                    # is what destroyed a whole queue on 2026-08-11.
                    log(f"  browser died during node {nid}"
                        + (f" ({nname})" if nname else "")
                        + " — NOT failing it; the claim will be swept back to the "
                          "queue. Relaunching.")
                    page = None          # forces the relaunch at the loop top
                    continue
                try:
                    requests.post(f"{base}/jobs/{nid}/status", headers=_auth(api_key),
                                  params={"backend": WORKER_BACKEND},
                                  json=status_body("failed", str(e)), timeout=30)
                except Exception:
                    pass
                log(f"  FAIL node {nid}" + (f" ({nname})" if nname else "") + f": {e}")
        time.sleep(poll_s)
