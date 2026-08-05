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

Promotion: also operator-triggered. Even with all variants chosen the run
STOPS before creating the video job; promote in the UI or rerun with
--resume-batch <id> --promote. Nothing renders until the operator says so.

Exit codes:
  0 OK | 1 unknown/server | 2 parse | 3 auth | 4 worker-offline/stall
  5 ingredient-binding | 6 duplicate-name | 7 not-ready | 8 image-gen-fail
  9 policy-terminal | 10 render-fail
"""
import argparse
import json
import os
import re
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


def missing_reference_bindings(md_text, has_subject, product_bound, ingredient_names):
    """v888 — every Ingredients row typed character/product with a non-empty
    Source declares an UPLOAD reference. Return the rows this invocation does
    not bind, so the run stops BEFORE import instead of shipping a build whose
    product renders as a generic bottle.

    Concrete failure this prevents: the boardwalk-betrayal v5 batch
    (d8697c58, 2026-08-02) went out without `--product`, so images 10-14 —
    every product, testimonial, purity and CTA frame — carried no Korella
    reference at all.

    Binding paths: persona alias rows <- --avatar/--subject; the single
    product row <- --product/--product-node; anything else <- --ingredient.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    import image_platform as ip
    parsed = ip.parse_scene_table(md_text)
    rows = parsed.get("ingredients") or []
    bound_names = {str(n).strip().lower() for n in (ingredient_names or [])}
    missing = []
    for row in rows:
        name = (row.get("name") or "").strip()
        rtype = (row.get("type") or "").strip().lower()
        source = (row.get("source") or "").strip()
        if rtype not in ("character", "product") or not source:
            continue
        if name.lower() in bound_names:
            continue
        if ip._is_persona_alias(name):
            if not has_subject:
                missing.append((name, rtype, source, "--avatar <name> (or --subject <id>)"))
            continue
        if rtype == "product":
            if not product_bound:
                missing.append((name, rtype, source, "--product <name> (or --product-node <id>)"))
            continue
        if re.match(r"^\(?\s*no\s+upload\s*\)?$", source, re.I):
            # v618b discipline: a character/product row with no real upload is
            # mis-typed — steer to the fix, not to a binding flag.
            missing.append((name, rtype, source,
                            "no upload exists: change the row to type=extra + Source inline (v618b)"))
            continue
        missing.append((name, rtype, source, f'--ingredient "{name}=<name-or-id>"'))
    return missing


# ---------------------------------------------------------------------------
# v888.1 — generic reference resolver
#
# Three name spaces rot at different speeds: row names live forever in the
# build, aliases (~/.kaveno/aliases.json) are re-pointable, node ids die when
# uploads are deleted. The old flow hard-wired across them (a 5-string
# persona-alias set decided which row --avatar credits), so a build whose
# persona row is named "the woman" needed the SAME node bound twice under two
# flags. This resolver bridges the layers per row, most explicit wins:
#
#   1. --ingredient "name=ref"          (explicit, always wins)
#   2. the row's own Source cell        ("upload node 4481" / "upload elder71")
#   3. a saved alias matching the row name
#   4. base-name retry                  ("the man (day 30)" -> "the man")
#   5. slot inference, only when unambiguous (--avatar -> the sole unbound
#      character row; --product -> the sole unbound product row)
#   6. nothing guessed -> the v888 guard still refuses with exact flags
#
# Every resolved node is liveness-checked against the live uploads list, so a
# deleted upload fails with the row + stale id named instead of a server 400.
# ---------------------------------------------------------------------------

_SOURCE_NODE_RE = re.compile(r"^\s*upload(?:ed)?\s*(?:node\s*)?#?(\d+)\s*$", re.I)
_SOURCE_ALIAS_RE = re.compile(r"^\s*upload(?:ed)?[:\s]\s*([A-Za-z][\w][\w .-]*?)\s*$", re.I)
_SOURCE_BARE_NODE_RE = re.compile(r"^\s*node\s*#?(\d+)\s*$", re.I)
_STATE_SUFFIX_RE = re.compile(r"^(.+?)\s*\([^)]+\)\s*$")


def parse_source_ref(source):
    """Parse an Ingredients Source cell into ('node', id) / ('alias', name) / None.

    Canonical forms (documented in the build checklist): `upload <alias>` is
    preferred (aliases survive re-uploads), `upload node <id>` accepted.
    Anything else (inline, (no upload), free prose) returns None and the
    cascade continues.
    """
    s = (source or "").strip().strip("`")
    m = _SOURCE_NODE_RE.match(s) or _SOURCE_BARE_NODE_RE.match(s)
    if m:
        return ("node", int(m.group(1)))
    m = _SOURCE_ALIAS_RE.match(s)
    if m and not m.group(1).strip().isdigit():
        return ("alias", m.group(1).strip())
    return None


def _strip_state_suffix(name):
    """'the man (day 30)' -> 'the man'; None when there is no (state) suffix."""
    m = _STATE_SUFFIX_RE.match(name)
    return m.group(1).strip() if m else None


def _declared_upload_rows(md_text):
    """The Ingredients rows that declare an upload (v618b: character/product
    with a non-empty Source cell)."""
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    import image_platform as ip
    parsed = ip.parse_scene_table(md_text)
    rows = []
    for row in parsed.get("ingredients") or []:
        name = (row.get("name") or "").strip()
        rtype = (row.get("type") or "").strip().lower()
        source = (row.get("source") or "").strip()
        if name and rtype in ("character", "product") and source:
            rows.append({"name": name, "type": rtype, "source": source})
    return rows


def plan_reference_bindings(rows, explicit, aliases, uploads_by_id,
                            subject, product_node):
    """Pure planner (no network, unit-testable). Returns
    (bindings, new_subject, new_product, notes, errors):
      bindings     {row_name: node_id} to add as --ingredient pairs
      new_subject  subject id inferred from the sole character row, or None
      new_product  product id inferred from the sole product row, or None
      notes        human lines explaining every resolution
      errors       hard problems (stale node, unknown alias) — abort, send nothing
    """
    bindings, notes, errors = {}, [], []
    unbound_chars, unbound_products = [], []

    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    import image_platform as ip

    def _alias_lookup(key):
        return aliases.get(key.lower())

    for row in rows:
        name, rtype, source = row["name"], row["type"], row["source"]
        low = name.lower()
        if low in explicit:
            continue  # --ingredient wins; guard already credits it
        if rtype == "character" and ip._is_persona_alias(name) and subject:
            continue  # legacy path: server binds this row from subject_node_id
        if rtype == "product" and product_node:
            continue  # server binds the product row from product_node_id

        node, how = None, None
        ref = parse_source_ref(source)
        if ref:
            kind, val = ref
            if kind == "node":
                node, how = val, f"Source cell 'upload node {val}'"
            else:
                nid = _alias_lookup(val)
                if nid:
                    node, how = nid, f"Source cell alias '{val}'"
                else:
                    errors.append(
                        f"row '{name}': Source cell names alias '{val}' but no such alias "
                        f"is saved — run: send_to_platform.py set-alias {val} <node_id>")
                    continue
        if node is None:
            nid = _alias_lookup(low)
            if nid:
                node, how = nid, f"saved alias '{low}'"
        if node is None:
            base = _strip_state_suffix(name)
            if base:
                bl = base.lower()
                if bl in explicit:
                    node, how = explicit[bl], f"base name '{base}' bound via --ingredient"
                else:
                    nid = _alias_lookup(bl)
                    if nid:
                        node, how = nid, f"base-name alias '{bl}'"
        if node is None:
            (unbound_chars if rtype == "character" else unbound_products).append(name)
            continue

        if uploads_by_id is not None and node not in uploads_by_id:
            errors.append(
                f"row '{name}': {how} -> node {node}, but that upload no longer exists "
                f"(deleted?) — run list-uploads, then fix the Source cell or re-point the alias")
            continue
        bindings[name] = node
        notes.append(f"{name} -> node {node} ({how})")

    # slot inference — only ever when there is exactly ONE candidate (v573:
    # at most one character upload + one product upload exist per build, so
    # a sole unbound row is unambiguous; two rows are never guessed).
    if subject and len(unbound_chars) == 1:
        bindings[unbound_chars[0]] = subject
        notes.append(f"{unbound_chars[0]} -> node {subject} (--avatar/--subject, sole unbound character row)")
        unbound_chars = []
    if product_node and len(unbound_products) == 1:
        bindings[unbound_products[0]] = product_node
        notes.append(f"{unbound_products[0]} -> node {product_node} (--product, sole unbound product row)")
        unbound_products = []

    # reverse inference: a zero-flag send whose build self-describes its
    # uploads still needs subject_node_id / product_node_id for the import.
    new_subject = new_product = None
    if not subject:
        char_bound = [(r["name"], bindings[r["name"]]) for r in rows
                      if r["type"] == "character" and r["name"] in bindings]
        if len(char_bound) == 1:
            new_subject = char_bound[0][1]
            notes.append(f"subject <- node {new_subject} (sole character row '{char_bound[0][0]}')")
    if not product_node:
        prod_bound = [(r["name"], bindings[r["name"]]) for r in rows
                      if r["type"] == "product" and r["name"] in bindings]
        if len(prod_bound) == 1:
            new_product = prod_bound[0][1]
            notes.append(f"product <- node {new_product} (sole product row '{prod_bound[0][0]}')")

    return bindings, new_subject, new_product, notes, errors


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
    p.add_argument("--promote", action="store_true",
                   help="promote the batch to a video job once all variants are chosen (default is STOP — the operator triggers promotion)")
    p.add_argument("--resume-batch", help="skip import, resume from an existing batch id")
    p.add_argument("--no-render", action="store_true", help="stop after promote (don't poll clips)")
    p.add_argument("--bindings", action="store_true", dest="bindings_only",
                   help="resolve reference bindings, print the table, send NOTHING (dry run)")
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

        # v888.1 — generic reference resolver: bridge row names / aliases /
        # live nodes per row BEFORE the v888 guard, so a self-describing build
        # sends with zero flags and nothing is ever typed twice.
        if not args.resume_batch:
            rows = _declared_upload_rows(md_text)
            if rows:
                uploads_cache = uploads_cache or _fetch_uploads(client)
                uploads_by_id = {n["id"]: n for n in uploads_cache}
                explicit = {}
                for item in args.ingredient:
                    name, _, nid = item.partition("=")
                    if nid.strip().isdigit():
                        explicit[name.strip().lower()] = int(nid)
                bindings, new_subject, new_product, notes, errors = plan_reference_bindings(
                    rows, explicit, _load_aliases(), uploads_by_id,
                    args.subject, args.product_node)
                for msg in notes:
                    print(f"  bind: {msg}", flush=True)
                if errors:
                    raise PlatformError(
                        EXIT_INGREDIENT,
                        "REFERENCE RESOLVE (nothing sent):\n"
                        + "\n".join("  - " + e for e in errors))
                for name, nid in bindings.items():
                    args.ingredient.append(f"{name}={nid}")
                if new_subject and not args.subject:
                    args.subject = new_subject
                if new_product and not args.product_node:
                    args.product_node = new_product
            if args.bindings_only:
                print("bindings dry-run complete — nothing sent", flush=True)
                return EXIT_OK

        if args.resume_batch:
            batch_id = args.resume_batch
        else:
            if not args.subject:
                print("tip: --avatar <name-or-alias> works too (see list-uploads)", file=sys.stderr, flush=True)
                raise PlatformError(EXIT_UNKNOWN, "--subject <upload node id> is required for import")
            # v888 — refuse to import a build whose declared upload references
            # are not all bound by this invocation (see missing_reference_bindings).
            gaps = missing_reference_bindings(
                md_text,
                has_subject=bool(args.subject),
                product_bound=bool(args.product_node),
                ingredient_names=[i.partition("=")[0] for i in (args.ingredient or [])],
            )
            if gaps:
                lines = [f"  - {n!r} (type={t}, Source: {s}) -> pass {flag}"
                         for n, t, s, flag in gaps]
                raise PlatformError(
                    EXIT_INGREDIENT,
                    "REFERENCE BINDING: the Ingredients table declares uploaded "
                    "references this command does not bind. Nothing was sent.\n"
                    + "\n".join(lines))
            report["stages"].append("refbind:ok")
            print(f"reference bindings: OK (subject={args.subject}"
                  + (f", product={args.product_node}" if args.product_node else "")
                  + ")", flush=True)
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

        # promotion is OPERATOR-TRIGGERED (2026-08-03) — never automatic
        if not args.promote:
            report["stages"].append("promote:awaiting_operator")
            print(f"batch ready — all variants chosen. Promote it yourself in the UI, or run:\n"
                  f"  python send_to_platform.py x --resume-batch {batch_id} --promote", flush=True)
            return EXIT_OK

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
