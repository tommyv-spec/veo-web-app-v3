#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""send_to_platform.py — trigger the platform pipeline from the CLI (v886).

Pipeline: pre-flight (local parse) -> import -> poll image gen (auto-choose
variants) -> promote to video job -> poll clips -> classified report.

Auth: Authorization: Bearer <UserWorkerToken>  (env KAVENO_API_TOKEN or --token).
Mint a token in the UI or POST /api/user-worker/tokens/generate.
No token set? The CLI finds one by itself, in order: --token > KAVENO_API_TOKEN
> VEO_TOKEN > USER_WORKER_TOKEN (env or ~/veo-worker/.env, the flow worker's
own token) > ~/.kaveno/token (saved via `set-token <token>`).

Usage:
  python send_to_platform.py videos/build.md --subject 42 --name "8-2.1 Test"
  python send_to_platform.py videos/build.md --subject 42 --review
  python send_to_platform.py list-uploads
  python send_to_platform.py set-token <token>   # save once, forget forever
  python send_to_platform.py set-alias nuri 1313 # name an upload once...
  python send_to_platform.py videos/build.md --avatar nuri --product korella

Variant approval: the operator picks variants in the UI (default). The run
stops when images are ready and prints the resume command; continue with
--resume-batch <id> after choosing. Pass --auto-choose to let the CLI take
variant 1 of every image unattended.

Exit codes:
  0 OK | 1 unknown/server | 2 parse | 3 auth | 4 worker-offline/stall
  5 ingredient-binding | 6 duplicate-name | 7 not-ready | 8 image-gen-fail
  9 policy-terminal | 10 render-fail
"""
import argparse
import json
import os
import sys
import time

DEFAULT_URL = "https://kavenobuilder.com"

EXIT_OK = 0
EXIT_UNKNOWN = 1
EXIT_PARSE = 2
EXIT_AUTH = 3
EXIT_WORKER = 4
EXIT_INGREDIENT = 5
EXIT_DUPLICATE = 6
EXIT_NOT_READY = 7
EXIT_IMAGE_FAIL = 8
EXIT_POLICY = 9
EXIT_RENDER_FAIL = 10

# flow_worker terminal policy reasons surface inside clip error_message text
POLICY_MARKERS = (
    "PROMINENT_PEOPLE", "SEXUAL", "CSAM", "REPUTATIONAL", "MISREPRESENT",
    "MINOR", "prominent person", "content policy", "flagged",
)


class PlatformError(Exception):
    def __init__(self, exit_code, message, detail=None):
        super().__init__(message)
        self.exit_code = exit_code
        self.message = message
        self.detail = detail


def normalize_detail(resp):
    """Platform quirk: JSON `detail` is a str on most errors but a dict on
    import 409 / promote 409 / job-create 400. Returns (message, dict|None)."""
    try:
        body = resp.json()
    except ValueError:
        return (resp.text or "")[:500], None
    if not isinstance(body, dict):
        return str(body)[:500], None
    detail = body.get("detail", body)
    if isinstance(detail, dict):
        msg = detail.get("message") or detail.get("error") or json.dumps(detail)[:300]
        return msg, detail
    return str(detail), None


def classify_http_error(resp):
    msg, det = normalize_detail(resp)
    code = resp.status_code
    if code in (401, 403):
        return PlatformError(EXIT_AUTH, f"AUTH ({code}): {msg}", det)
    if code == 409:
        if det and det.get("error") == "duplicate_batch_name":
            return PlatformError(EXIT_DUPLICATE, f"DUPLICATE_NAME: {msg}", det)
        return PlatformError(EXIT_NOT_READY, f"NOT_READY (409): {msg}", det)
    if code == 400:
        if msg.startswith("Parse error:"):
            return PlatformError(EXIT_PARSE, msg, det)
        if "Unresolved ingredients" in msg or "type=character/product" in msg:
            return PlatformError(EXIT_INGREDIENT, f"INGREDIENT_BINDING: {msg}", det)
        return PlatformError(EXIT_UNKNOWN, f"HTTP 400: {msg}", det)
    return PlatformError(EXIT_UNKNOWN, f"HTTP {code}: {msg}", det)


_SAVED_TOKEN_PATH = os.path.join(os.path.expanduser("~"), ".kaveno", "token")


_TOKEN_KEYS = ("KAVENO_API_TOKEN", "VEO_TOKEN", "USER_WORKER_TOKEN")


def _read_env_file_token(path):
    """Pull a worker token out of a worker .env file (KEY=value lines)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                key, sep, val = line.strip().partition("=")
                if sep and key.strip() in _TOKEN_KEYS:
                    val = val.strip().strip("\"'")
                    if val:
                        return val
    except OSError:
        pass
    return None


def resolve_token(cli_token):
    """Find a worker token without making the operator think about it.
    Order: --token > KAVENO_API_TOKEN > VEO_TOKEN > the flow worker's own
    ~/veo-worker/.env > the token saved by `set-token`."""
    if cli_token:
        return cli_token, "--token"
    for env_key in _TOKEN_KEYS:
        val = os.environ.get(env_key, "").strip()
        if val:
            return val, f"env {env_key}"
    val = _read_env_file_token(os.path.join(os.path.expanduser("~"), "veo-worker", ".env"))
    if val:
        return val, "~/veo-worker/.env (flow worker token)"
    try:
        with open(_SAVED_TOKEN_PATH, encoding="utf-8") as f:
            val = f.read().strip()
        if val:
            return val, "~/.kaveno/token"
    except OSError:
        pass
    return None, None


def cmd_set_token(token):
    token = (token or "").strip()
    if len(token) < 20:
        print("that does not look like a token (too short) — copy the full one "
              "from https://kavenobuilder.com/static/my-worker.html", file=sys.stderr)
        return EXIT_AUTH
    os.makedirs(os.path.dirname(_SAVED_TOKEN_PATH), exist_ok=True)
    with open(_SAVED_TOKEN_PATH, "w", encoding="utf-8") as f:
        f.write(token + "\n")
    print(f"saved to {_SAVED_TOKEN_PATH} — every send_to_platform.py call finds it now")
    return EXIT_OK


def _as_list(data, key):
    """Endpoint may return a bare list or {key: [...]} — anything else is a
    contract break we want to fail loudly on, not iterate dict keys."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        val = data.get(key)
        if isinstance(val, list):
            return val
    raise PlatformError(EXIT_UNKNOWN, f"unexpected response shape for '{key}': {str(data)[:200]}")


def preflight_text(md_text):
    """Run the platform's OWN parser on the markdown, offline.
    Returns (ok, error_string). Catches every 'Parse error:' before any HTTP."""
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    import image_platform as ip
    try:
        ip.parse_scene_table(md_text)
        return True, ""
    except ValueError as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# HTTP client + pipeline stages
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, base_url, token):
        import requests  # deferred so tests of pure helpers don't need it
        self._rq = requests
        base = base_url.rstrip("/")
        # never send the bearer token over plaintext, except local dev
        if base.startswith("http://") and not any(
                h in base for h in ("localhost", "127.0.0.1")):
            raise PlatformError(EXIT_AUTH, f"AUTH: refusing to send token over http:// ({base}) — use https")
        self.base = base
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token}"

    def _check(self, resp):
        if resp.status_code >= 400:
            raise classify_http_error(resp)
        return resp.json() if resp.content else {}

    def get(self, path, **kw):
        try:
            return self._check(self.s.get(self.base + path, timeout=60, **kw))
        except self._rq.exceptions.Timeout:
            raise PlatformError(EXIT_WORKER, f"STALL: request timed out: GET {path}")

    def post(self, path, payload=None, **kw):
        try:
            return self._check(self.s.post(self.base + path, json=payload, timeout=120, **kw))
        except self._rq.exceptions.Timeout:
            raise PlatformError(EXIT_WORKER, f"STALL: request timed out: POST {path}")


def check_health(client, report):
    """GET /api/health (public) + both worker liveness endpoints.
    Renders/image-gen only happen while workers poll — server just queues."""
    health = client.get("/api/health")
    report["health"] = health.get("status")
    for key, path in (("flow_worker", "/api/user-worker/status"),
                      ("image_worker", "/api/images/worker/status")):
        try:
            report[key] = client.get(path)
        except PlatformError as e:
            report[key] = {"error": e.message}


def do_import(client, md_text, args, report):
    payload = {
        "markdown": md_text,
        "subject_node_id": args.subject,
        "n_variants": args.variants,
    }
    if args.name:
        payload["name_prefix"] = args.name
    if args.ingredient:
        pairs = {}
        for item in args.ingredient:
            name, _, nid = item.partition("=")
            if not nid.isdigit():
                raise PlatformError(EXIT_UNKNOWN, f"--ingredient wants Name=nodeId, got: {item}")
            pairs[name] = int(nid)
        payload["ingredient_node_ids"] = pairs
    if args.product_node:
        payload["product_node_id"] = args.product_node
    res = client.post("/api/images/import-scene-table", payload)
    report["import"] = {k: res.get(k) for k in
                        ("batch_id", "batch_name", "format", "created", "queued",
                         "waiting_on_parent", "scene_assignments_created")}
    report["scene_nodes"] = res.get("scene_nodes", {})
    return res["batch_id"]


def _upload_image_url(base, node):
    """Browser-viewable URL of an upload node's image (open while logged in)."""
    variants = node.get("variants") or []
    chosen = node.get("chosen_variant")
    v = chosen or (variants[0] if variants else None)
    if v and v.get("image_url"):
        return base + v["image_url"]
    return None


def _fetch_uploads(client):
    data = client.get("/api/images/nodes", params={"since_days": 3650})
    nodes = _as_list(data, "nodes")
    return [n for n in nodes if n.get("kind") == "upload"]


ALIASES_PATH = os.path.join(os.path.expanduser("~"), ".kaveno", "aliases.json")


def _load_aliases():
    try:
        with open(ALIASES_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def cmd_set_alias(name, node_id):
    if not name or not node_id or not str(node_id).isdigit():
        print("usage: send_to_platform.py set-alias <name> <node_id>", file=sys.stderr)
        return EXIT_UNKNOWN
    aliases = _load_aliases()
    aliases[name.lower()] = int(node_id)
    os.makedirs(os.path.dirname(ALIASES_PATH), exist_ok=True)
    with open(ALIASES_PATH, "w", encoding="utf-8") as f:
        json.dump(aliases, f, indent=2)
    print(f"alias '{name}' -> node {node_id} saved ({ALIASES_PATH})")
    return EXIT_OK


def resolve_upload_ref(client, ref, uploads=None):
    """Turn a human ref (alias, name fragment, or numeric id) into a node id.
    Saved alias wins; then exact name match; then substring. Several uploads
    with the SAME name -> newest (highest id), with a note. Different names
    matching -> error listing candidates with viewable URLs."""
    ref = str(ref).strip()
    if ref.isdigit():
        return int(ref)
    alias = _load_aliases().get(ref.lower())
    if alias:
        return alias
    if uploads is None:
        uploads = _fetch_uploads(client)
    low = ref.lower()
    exact = [n for n in uploads if (n.get("name") or "").lower() == low]
    hits = exact or [n for n in uploads if low in (n.get("name") or "").lower()]
    if not hits:
        raise PlatformError(EXIT_UNKNOWN,
                            f"no upload matches '{ref}' — run list-uploads to see names, "
                            f"or save one: send_to_platform.py set-alias {ref} <node_id>")
    names = {(n.get("name") or "") for n in hits}
    if len(names) > 1:
        lines = "\n".join(f"    node {n['id']}: {n.get('name')}" for n in hits[:8])
        raise PlatformError(EXIT_UNKNOWN,
                            f"'{ref}' matches several different uploads — pick one:\n{lines}")
    best = max(hits, key=lambda n: n["id"])
    if len(hits) > 1:
        print(f"  '{ref}': {len(hits)} uploads share this name — using newest (node {best['id']})", flush=True)
    return best["id"]


def cmd_list_uploads(client, as_json):
    data = client.get("/api/images/nodes", params={"since_days": 3650})
    nodes = _as_list(data, "nodes")
    uploads = [n for n in nodes if n.get("kind") == "upload"]
    if as_json:
        print(json.dumps([{k: n.get(k) for k in ("id", "name", "status")} for n in uploads], indent=2))
    else:
        if not uploads:
            print("no upload nodes found")
        aliases = _load_aliases()
        by_id = {v: k for k, v in aliases.items()}
        for n in uploads:
            print(f"  node {n['id']:>5}  {n.get('status', '?'):<8} {n.get('name', '')}")
            tag = f"  alias: {by_id[n['id']]}" if n["id"] in by_id else ""
            url = _upload_image_url(client.base, n)
            if url or tag:
                print(f"        {'view: ' + url if url else ''}{tag}")
    return EXIT_OK


def poll_images(client, batch_id, args, report):
    """Poll batch nodes until every generated node is ready+chosen.
    Auto-chooses variant 1 of each ready node (server then auto-queues draft
    children). --review stops before choosing so the operator picks in the UI."""
    deadline = time.time() + args.timeout_min * 60
    last_change = time.time()
    last_sig = None
    while True:
        data = client.get("/api/images/nodes", params={"batch_id": batch_id, "since_days": 30})
        nodes = _as_list(data, "nodes")
        gen = [n for n in nodes if n.get("kind") == "generated"]
        if not gen:
            raise PlatformError(EXIT_IMAGE_FAIL,
                                f"IMAGE_GEN_FAIL: batch {batch_id} has no generated nodes "
                                f"(wrong --resume-batch id?)")
        failed = [n for n in gen if n.get("status") == "failed"]
        ready_unchosen = [n for n in gen if n.get("status") == "ready" and not n.get("chosen_variant_id")]
        done = [n for n in gen if n.get("status") == "ready" and n.get("chosen_variant_id")]

        sig = tuple(sorted((n["id"], n.get("status"), bool(n.get("chosen_variant_id"))) for n in gen))
        if sig != last_sig:
            last_sig = sig
            last_change = time.time()
            print(f"  images: {len(done)}/{len(gen)} ready+chosen, "
                  f"{len(ready_unchosen)} awaiting choice, {len(failed)} failed", flush=True)

        if failed:
            report["image_failures"] = [
                {"node_id": n["id"], "name": n.get("name"), "error": n.get("error_message")}
                for n in failed]
            raise PlatformError(
                EXIT_IMAGE_FAIL,
                "IMAGE_GEN_FAIL: " + "; ".join(
                    f"node {n['id']}: {n.get('error_message') or 'no message'}" for n in failed))

        if ready_unchosen:
            if args.review:
                report["awaiting_review"] = [n["id"] for n in ready_unchosen]
                print(f"  --review: {len(ready_unchosen)} nodes ready — pick variants in the UI, "
                      f"then rerun with --resume-batch {batch_id}")
                return False
            for n in ready_unchosen:
                variants = n.get("variants") or []
                first_variant_id = variants[0].get("id") if variants else None
                if not first_variant_id:
                    raise PlatformError(EXIT_IMAGE_FAIL,
                                        f"IMAGE_GEN_FAIL: node {n['id']} ready but has no usable variants")
                client.post(f"/api/images/nodes/{n['id']}/choose",
                            {"variant_id": first_variant_id})
            continue  # re-poll immediately: choosing may queue draft children

        if gen and len(done) == len(gen):
            return True

        if time.time() > deadline:
            raise PlatformError(EXIT_WORKER, f"STALL: image gen not finished after {args.timeout_min} min")
        if time.time() - last_change > args.stall_min * 60:
            raise PlatformError(
                EXIT_WORKER,
                f"STALL: no image progress for {args.stall_min} min — is the image worker running? "
                f"(GET /api/images/worker/status)")
        time.sleep(args.poll_interval)


def promote(client, batch_id, report):
    res = client.post(f"/api/images/batches/{batch_id}/promote-to-video")
    report["promote"] = res
    job_id = res.get("video_job_id")
    if not job_id:
        raise PlatformError(EXIT_UNKNOWN, f"promote response missing video_job_id: {str(res)[:200]}")
    return job_id


def classify_clip_failures(clips):
    """Split failed clips into policy-terminal vs other render failures.
    flow_worker policy terminals arrive as clip error_message text."""
    policy, other = [], []
    for c in clips:
        if c.get("status") != "failed":
            continue
        text = f"{c.get('error_code') or ''} {c.get('error_message') or ''}"
        (policy if any(m.lower() in text.lower() for m in POLICY_MARKERS) else other).append(c)
    return policy, other


def poll_render(client, job_id, args, report):
    deadline = time.time() + args.render_timeout_min * 60
    last_progress = (None, time.time())
    while True:
        job = client.get(f"/api/jobs/{job_id}")
        status = job.get("status")
        pct = job.get("progress_percent")
        if pct != last_progress[0]:
            last_progress = (pct, time.time())
            print(f"  render: {status} {pct}% "
                  f"({job.get('completed_clips')}/{job.get('total_clips')} clips, "
                  f"{job.get('failed_clips')} failed)", flush=True)

        if status in ("completed", "failed", "cancelled"):
            data = client.get(f"/api/jobs/{job_id}/clips")
            clips = _as_list(data, "clips")
            policy, other = classify_clip_failures(clips)
            report["render"] = {
                "job_status": status,
                "completed_clips": job.get("completed_clips"),
                "failed_clips": job.get("failed_clips"),
                "policy_failures": [
                    {"clip_id": c.get("id"), "code": c.get("error_code"), "error": c.get("error_message")}
                    for c in policy],
                "other_failures": [
                    {"clip_id": c.get("id"), "code": c.get("error_code"), "error": c.get("error_message")}
                    for c in other],
            }
            if status == "completed" and not policy and not other:
                return EXIT_OK
            if policy:
                raise PlatformError(EXIT_POLICY, "POLICY_TERMINAL: " + "; ".join(
                    f"clip {c.get('id')}: {c.get('error_message')}" for c in policy))
            if other:
                raise PlatformError(EXIT_RENDER_FAIL, f"RENDER_FAIL: job {status}, " + "; ".join(
                    f"clip {c.get('id')}: {c.get('error_message')}" for c in other))
            raise PlatformError(EXIT_RENDER_FAIL, f"RENDER_FAIL: job {status}")

        if time.time() > deadline:
            raise PlatformError(EXIT_WORKER,
                                f"STALL: render not finished after {args.render_timeout_min} min "
                                f"— is flow_worker running? (GET /api/user-worker/status)")
        if time.time() - last_progress[1] > args.stall_min * 60:
            # progress stalled — only fatal if the flow worker looks dead too
            try:
                client.get("/api/user-worker/status")
            except PlatformError:
                raise PlatformError(EXIT_WORKER, "STALL: no render progress and flow_worker unreachable")
        time.sleep(args.poll_interval)


def main(argv=None):
    p = argparse.ArgumentParser(description="Send a videos/*.md build to the platform and render it.")
    p.add_argument("md_file", help="path to videos/<build>.md, or the literal 'list-uploads'")
    p.add_argument("token_value", nargs="?", help="the token (only with set-token)")
    p.add_argument("extra_value", nargs="?", help="node id (only with set-alias)")
    p.add_argument("--avatar", help="persona upload by NAME or alias (instead of --subject id)")
    p.add_argument("--product", help="product upload by NAME or alias (instead of --product-node id)")
    p.add_argument("--subject", type=int, help="upload node id of the persona (see list-uploads)")
    p.add_argument("--name", help="name_prefix for the batch (short label)")
    p.add_argument("--ingredient", action="append", default=[], metavar="NAME=NODEID")
    p.add_argument("--product-node", type=int, help="upload node id of the product (v583 shortcut)")
    p.add_argument("--variants", type=int, default=4, choices=range(1, 5))
    p.add_argument("--url", default=os.environ.get("KAVENO_URL", DEFAULT_URL))
    p.add_argument("--token", default=os.environ.get("KAVENO_API_TOKEN", ""))
    p.add_argument("--review", action="store_true", help="stop before variant auto-choice")
    p.add_argument("--auto-choose", action="store_true", dest="auto_choose",
                   help="pick variant 1 automatically (default is STOP and let the operator choose in the UI)")
    p.add_argument("--resume-batch", help="skip import, resume from an existing batch id")
    p.add_argument("--no-render", action="store_true", help="stop after promote (don't poll clips)")
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--json", action="store_true", dest="as_json")
    p.add_argument("--poll-interval", type=int, default=15)
    p.add_argument("--timeout-min", type=int, default=45, help="image-gen phase timeout")
    p.add_argument("--render-timeout-min", type=int, default=90)
    p.add_argument("--stall-min", type=int, default=10)
    args = p.parse_args(argv)

    if args.md_file == "set-token":
        return cmd_set_token(args.token_value)
    if args.md_file == "set-alias":
        return cmd_set_alias(args.token_value, args.extra_value)

    report = {"stages": []}
    try:
        if not args.token:
            token, source = resolve_token(args.token)
        else:
            token, source = args.token, "--token / KAVENO_API_TOKEN"
        if not token:
            print("hint: mint one at https://kavenobuilder.com/static/my-worker.html "
                  "(New token), then run: python send_to_platform.py set-token <token>",
                  file=sys.stderr, flush=True)
            raise PlatformError(EXIT_AUTH, "AUTH: no token — set KAVENO_API_TOKEN or pass --token")
        print(f"token: {source}", flush=True)
        args.token = token
        client = Client(args.url, args.token)

        if args.md_file == "list-uploads":
            return cmd_list_uploads(client, args.as_json)

        md_text = open(args.md_file, encoding="utf-8").read()

        if not args.skip_preflight:
            ok, err = preflight_text(md_text)
            if not ok:
                raise PlatformError(EXIT_PARSE, f"PRE-FLIGHT parse error (local, nothing sent): {err}")
            report["stages"].append("preflight:ok")
            print("preflight: parse OK", flush=True)

        check_health(client, report)
        report["stages"].append("health:ok")

        # names/aliases -> node ids (so the operator never needs the numbers)
        uploads_cache = None
        if args.avatar and not args.subject:
            uploads_cache = uploads_cache or _fetch_uploads(client)
            args.subject = resolve_upload_ref(client, args.avatar, uploads_cache)
            print(f"avatar '{args.avatar}' -> node {args.subject}", flush=True)
        if args.product and not args.product_node:
            uploads_cache = uploads_cache or _fetch_uploads(client)
            args.product_node = resolve_upload_ref(client, args.product, uploads_cache)
            print(f"product '{args.product}' -> node {args.product_node}", flush=True)
        if args.ingredient:
            resolved = []
            for item in args.ingredient:
                name, sep, val = item.partition("=")
                if sep and val and not val.strip().isdigit():
                    uploads_cache = uploads_cache or _fetch_uploads(client)
                    nid = resolve_upload_ref(client, val, uploads_cache)
                    print(f"ingredient '{name}' = '{val}' -> node {nid}", flush=True)
                    item = f"{name}={nid}"
                resolved.append(item)
            args.ingredient = resolved

        if args.resume_batch:
            batch_id = args.resume_batch
        else:
            if not args.subject:
                print("tip: --avatar <name-or-alias> works too (see list-uploads)", file=sys.stderr, flush=True)
                raise PlatformError(EXIT_UNKNOWN, "--subject <upload node id> is required for import")
            batch_id = do_import(client, md_text, args, report)
            print(f"import: batch {batch_id}", flush=True)
        report["batch_id"] = batch_id
        report["stages"].append("import:ok")

        # operator approves variants BY DEFAULT (2026-08-03) — auto-choose is opt-in
        if not args.auto_choose:
            args.review = True

        if not poll_images(client, batch_id, args, report):
            report["stages"].append("images:awaiting_review")
            return EXIT_OK
        report["stages"].append("images:ok")

        job_id = promote(client, batch_id, report)
        report["job_id"] = job_id
        print(f"promote: video job {job_id}", flush=True)
        report["stages"].append("promote:ok")

        if args.no_render:
            return EXIT_OK

        rc = poll_render(client, job_id, args, report)
        report["stages"].append("render:ok")
        return rc

    except PlatformError as e:
        report["error"] = {"exit_code": e.exit_code, "message": e.message, "detail": e.detail}
        print(f"ERROR: {e.message}", file=sys.stderr, flush=True)
        return e.exit_code
    except Exception as e:  # network errors, unexpected
        report["error"] = {"exit_code": EXIT_UNKNOWN, "message": f"{type(e).__name__}: {e}"}
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        return EXIT_UNKNOWN
    finally:
        if 'args' in dir() and getattr(args, "as_json", False) and args.md_file != "list-uploads":
            print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
