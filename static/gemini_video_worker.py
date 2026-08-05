#!/usr/bin/env python3
"""
gemini_video_worker.py — standalone Gemini web VIDEO (Veo) worker.

Drives gemini.google.com's own private endpoints, called from INSIDE a real
Chrome page. The browser mints every cookie / x-browser-validation / x-client-data
header itself, so there is no token reversing and no paid API cost — the video
quota is the one included in the operator's Google plan.

Same risk posture + session model as flow_worker.py and chatgpt_image_worker.py:
its OWN Chrome profile, one-time manual login, headful, human-paced.

--------------------------------------------------------------------------------
ENDPOINT CHAIN (reverse-engineered from gemini.google.com.har, 2026-08-03)
--------------------------------------------------------------------------------
  1. UPLOAD start frame        POST push.clients6.google.com/upload/
                               x-goog-upload-protocol: resumable  (2 steps)
                               -> "/contrib_service/ttl_1d/<id>"
  2. SUBMIT prompt             POST gemini.google.com/_/BardChatUi/data/
                                    assistant.lamda.BardFrontendService/StreamGenerate
                               f.req = [null, "<json>"] , at = <SNlM0e>
                               -> conversation "c_<hex16>", response "r_<hex16>",
                                  and key "65" = [[chip_url], "<video job uuid>"]
  3. POLL job                  POST .../batchexecute?rpcids=kwDCne
                               f.req = [[["kwDCne","[\\"<uuid>\\"]",null,"generic"]]]
  4. FETCH conversation        POST .../batchexecute?rpcids=hNvQHb
                               f.req payload "[\\"c_<hex16>\\",10,null,1,[0],[4],null,1]"
                               -> key "60" holds the finished asset, including
                                  https://contribution.usercontent.google.com/download?c=...
  5. DOWNLOAD mp4              GET that URL (CORS allows gemini.google.com +
                               credentials, so an in-page fetch works)

Quota probe (bonus, rpcid qpEbW, payload "[[[6,5]]]") -> [[[null,5],used,limit,...]].

--------------------------------------------------------------------------------
SETUP (once)
--------------------------------------------------------------------------------
  pip install patchright && patchright install chromium

  # 1. SESSION — same method as the ChatGPT worker: the account's live session is
  #    COPIED out of a NON-STABLE Chrome channel (Beta/Dev/Canary) into a clean
  #    per-account profile, so there is no manual login. Your daily stable Chrome
  #    is never read, never closed. Just pass --email on any command:
  python code/static/gemini_video_worker.py --email you@gmail.com --quota

  #    Fallback if the account is not in a non-stable channel — log in once by hand:
  python code/static/gemini_video_worker.py --login

  # 2. generate (start frame optional — text-to-video works too)
  python code/static/gemini_video_worker.py \
      --ref clip_25_start.png --prompt "she is talking" --out out.mp4

  # 3. manual mode: worker opens the window, YOU type/attach/send in the UI,
  #    the worker sniffs the job off the network and does poll + download.
  #    Zero selectors -> immune to Gemini UI changes. Use when --prompt fails.
  python code/static/gemini_video_worker.py --manual --out out.mp4

  # quota only
  python code/static/gemini_video_worker.py --quota

--------------------------------------------------------------------------------
EMERGENCY MODE (--serve) — stand in for flow_worker.py
--------------------------------------------------------------------------------
  python code/static/gemini_video_worker.py --email you@gmail.com \
      --serve --token <worker token from My Worker -> Advanced>

Claims the SAME queue flow_worker.py claims: GET /api/user-worker/jobs/pending
(server-side filter is Job.backend == 'flow'), then per clip —
  fetch start/end frame -> clip_status 'generating' -> Gemini render ->
  POST /jobs/{id}/upload-video/{clip_index} -> clip_status 'completed'
and finally job_status 'completed' (or 'failed' listing the bad clips).

Run it when the main Flow worker cannot run. Do NOT run both at once against the
same account — they claim from one queue and would fight over the same clips.

KNOWN GAPS vs flow_worker.py (deliberate — this is the basic version):
  * END FRAME is passed as a second reference image plus a wording instruction.
    Flow has a real interpolation control; Gemini's chat surface does not expose
    one. Treat end-frame fidelity as UNVERIFIED until you eyeball a render.
  * DURATION is whatever Gemini returns (~8-10s observed). The job's requested
    4/6/8/10 is logged, not enforced — there is no duration control here.
  * No variants (Flow renders 2+ and picks), no policy-violation classifier, no
    golden-restore, no ghost detection, no auto-redo, no redo/kling queues.
  * One clip at a time, one account.
--------------------------------------------------------------------------------
"""

import argparse
import base64
import json
import os
import random
import re
import sys
import time
import uuid

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)          # so worker_profile_pull imports when run by path
PROFILE_DIR = os.environ.get("GEMINI_PROFILE_DIR", os.path.join(BASE_DIR, ".gemini_profile"))
GEMINI_URL = "https://gemini.google.com/app"
CHROME_CHANNEL = os.environ.get("WORKER_CHROME_CHANNEL", "chrome")

# Veo renders are slow. Observed in the HAR: submit 19:44:28 -> asset ready
# 19:45:45 (~77s) for an 8s clip. Allow a wide margin.
GEN_TIMEOUT_S = int(os.environ.get("GEMINI_GEN_TIMEOUT_S", "900"))
POLL_EVERY_S = float(os.environ.get("GEMINI_POLL_EVERY_S", "6"))

CHROME_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-focus-on-load",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--mute-audio",
]
_IGNORE_DEFAULT_ARGS = ["--enable-automation"]
if sys.platform == "darwin":
    _IGNORE_DEFAULT_ARGS += ["--password-store=basic", "--use-mock-keychain"]

MIME_BY_EXT = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".webp": "image/webp", ".gif": "image/gif", ".mp4": "video/mp4",
}


def log(msg):
    print(f"[gemini-video] {msg}", flush=True)


def jitter(a=1.0, b=2.5):
    time.sleep(random.uniform(a, b))


def _import_playwright():
    """Prefer Patchright (stealth); fall back to vanilla Playwright."""
    try:
        from patchright.sync_api import sync_playwright  # type: ignore
        log("Patchright ACTIVE (CDP-detection bypass on)")
        return sync_playwright
    except ImportError:
        from playwright.sync_api import sync_playwright  # type: ignore
        log("WARNING: patchright not installed — using vanilla Playwright "
            "(higher bot-detection risk). pip install patchright && patchright install chromium")
        return sync_playwright


# ---------------------------------------------------------------------------
# Session: COPY the logged-in profile, same method the ChatGPT worker uses
# (chatgpt_session_pull.py -> worker_profile_pull.build_lean_golden_from_profile).
# ---------------------------------------------------------------------------

# Channels the worker MAY copy from — everything EXCEPT stable, so the operator's
# daily Chrome is never closed or copied.
_NON_STABLE = ("chrome beta", "chrome dev", "chrome sxs", "chrome canary", "chromium")


def _non_stable_profile(email):
    """(user_data_dir, profile_folder) for `email` in a NON-STABLE Chrome channel,
    or (None, None). Never returns the stable channel."""
    import worker_profile_pull as wpp
    for ud in wpp.resolve_laptop_user_data_dirs():
        if not ud or not any(tag in ud.lower() for tag in _NON_STABLE):
            continue
        folder = wpp.find_profile_dir_for_email(ud, email)
        if folder:
            return ud, folder
    return None, None


def pull_session(email, golden_folder):
    """Build a golden copy of the non-stable-channel Chrome profile logged into
    `email`, which the worker then launches already signed in. Returns the launch
    channel string ("chrome-beta", …) on success, False on skip."""
    import worker_profile_pull as wpp
    ud, folder = _non_stable_profile(email)
    if not ud:
        log(f"session-pull: {email!r} is not logged in under a non-stable Chrome "
            f"channel. Log Gemini into CHROME BETA (separate from your daily "
            f"Chrome) so the worker can copy it, then re-run.")
        return False
    log(f"session-pull: copying {email} from {folder!r} in {ud} (non-stable channel)")
    # allow_channel_close=True is safe: close_chrome only ever closes the
    # NON-STABLE channel, never the operator's daily stable Chrome.
    return wpp.build_lean_golden_from_profile(
        email, golden_folder=golden_folder, label="GEMINI", user_data_dir=ud,
        close_chrome=lambda _u: wpp.close_laptop_chrome(ud, log=log),
        log=log, allow_channel_close=True)


def use_account(email):
    """Point the worker at a clean per-account profile and fill it by copying the
    live session. Sets PROFILE_DIR + CHROME_CHANNEL for this run."""
    global PROFILE_DIR, CHROME_CHANNEL
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", email.strip().lower())
    PROFILE_DIR = os.path.join(BASE_DIR, f".gemini_profile_{safe}")
    log(f"using per-account profile: {os.path.basename(PROFILE_DIR)}")
    channel = pull_session(email, PROFILE_DIR)
    if channel:
        CHROME_CHANNEL = channel
        log(f"session copied — launching with channel={channel}, no manual login needed.")
        return True
    log("no session copied; will wait for a one-time login in the worker's window.")
    return False


def launch(p, headless=False):
    os.makedirs(PROFILE_DIR, exist_ok=True)
    ctx = p.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        channel=CHROME_CHANNEL,
        ignore_default_args=_IGNORE_DEFAULT_ARGS,
        headless=headless,
        viewport={"width": 1280, "height": 900},
        args=list(CHROME_ARGS),
    )
    # navigator.clipboard.writeText needs this; without it _enter_prompt falls
    # back to character typing (correct, just slow).
    try:
        ctx.grant_permissions(["clipboard-read", "clipboard-write"],
                              origin="https://gemini.google.com")
    except Exception as e:
        log(f"clipboard permission not granted ({e.__class__.__name__}); "
            f"prompts will be typed instead of pasted")
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    return ctx, page


# ---------------------------------------------------------------------------
# In-page JS. Everything network-facing runs here so Chrome signs the requests.
# ---------------------------------------------------------------------------

JS_LIB = r"""
() => {
  if (window.__gvw) return true;

  const RPC_BASE = 'https://gemini.google.com/_/BardChatUi/data/batchexecute';
  const STREAM_URL = 'https://gemini.google.com/_/BardChatUi/data/' +
                     'assistant.lamda.BardFrontendService/StreamGenerate';

  // WIZ_global_data carries the per-session tokens the UI itself uses:
  //   SNlM0e -> the "at" CSRF token   cfb2h -> "bl" build label   FdrFJe -> f.sid
  function wiz(key, re) {
    try {
      const g = window.WIZ_global_data;
      if (g && g[key]) return g[key];
    } catch (e) {}
    const m = document.documentElement.innerHTML.match(re);
    return m ? m[1] : null;
  }
  const tokens = () => ({
    at:  wiz('SNlM0e', /"SNlM0e":"([^"]+)"/),
    bl:  wiz('cfb2h',  /"cfb2h":"([^"]+)"/),
    sid: wiz('FdrFJe', /"FdrFJe":"([^"]+)"/),
  });

  let reqid = Math.floor(Math.random() * 900000) + 100000;

  async function rpc(rpcid, payload, sourcePath) {
    const t = tokens();
    if (!t.at) throw new Error('no SNlM0e token on page (not logged in?)');
    reqid += 100000;
    const qs = new URLSearchParams({
      'rpcids': rpcid,
      'source-path': sourcePath || '/app',
      'bl': t.bl || '',
      'f.sid': t.sid || '',
      'hl': 'en',
      '_reqid': String(reqid),
    });
    const freq = JSON.stringify([[[rpcid, JSON.stringify(payload), null, 'generic']]]);
    const body = new URLSearchParams({ 'f.req': freq, 'at': t.at }).toString();
    const r = await fetch(RPC_BASE + '?' + qs.toString(), {
      method: 'POST', credentials: 'include',
      headers: {
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'x-same-domain': '1',
      },
      body,
    });
    return await r.text();
  }

  // ---- resumable upload -> "/contrib_service/ttl_1d/<id>" -----------------
  async function upload(name, b64, mime, size) {
    const bin = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
    const common = { 'x-tenant-id': 'bard-storage', 'push-id': 'feeds/mcudyrk2a4khkz' };
    const r1 = await fetch('https://push.clients6.google.com/upload/', {
      method: 'POST', credentials: 'include',
      headers: Object.assign({
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'x-goog-upload-command': 'start',
        'x-goog-upload-protocol': 'resumable',
        'x-goog-upload-header-content-length': String(bin.length),
      }, common),
      body: 'File name: ' + name,
    });
    // x-goog-upload-control-url is in access-control-expose-headers; the plain
    // x-goog-upload-url is not always, so prefer the control URL (same target).
    const putUrl = r1.headers.get('x-goog-upload-url') ||
                   r1.headers.get('x-goog-upload-control-url');
    if (!putUrl) throw new Error('upload start gave no upload URL (status ' + r1.status + ')');
    const r2 = await fetch(putUrl, {
      method: 'POST', credentials: 'include',
      headers: Object.assign({
        'x-goog-upload-command': 'upload, finalize',
        'x-goog-upload-offset': '0',
      }, common),
      body: bin,
    });
    const txt = (await r2.text()).trim();
    if (!txt.startsWith('/contrib_service/')) {
      throw new Error('upload finalize returned: ' + txt.slice(0, 200));
    }
    return txt;
  }

  // ---- submit the prompt (0, 1 or 2 frame references) --------------------
  // `refs` is an ARRAY: [start] for a plain animate, [start, end] when the job
  // carries an end frame (Flow's interpolation contract). The captured request
  // held ONE entry in this slot; the slot is a list, so a second entry is the
  // natural extension — but Gemini honouring it as a true END frame is NOT
  // verified. See gemini_worker_notes in the module docstring.
  async function submit(prompt, refs) {
    const t = tokens();
    if (!t.at) throw new Error('no SNlM0e token on page (not logged in?)');
    const list = (refs || []).filter(Boolean);
    const files = list.length ? list.map(r => [[r.id, 1, null, r.mime], r.name]) : null;

    // Slot map taken verbatim from the captured StreamGenerate request; slots
    // left null in the capture stay null here.
    const inner = new Array(97).fill(null);
    inner[0] = [prompt, 0, null, files, null, null, 0, null, null,
                [null, null, null, null, null, null, [[null, null, null, 2]]]];
    inner[1] = ['en'];
    inner[2] = ['', '', '', null, null, null, null, null, null, ''];
    inner[3] = '';                       // conversation context blob: empty = new chat
    inner[4] = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()))
                 .replace(/-/g, '').slice(0, 32);
    inner[6] = [0];  inner[7] = 1;   inner[10] = 1;  inner[11] = 0;
    inner[17] = [[0]]; inner[18] = 0; inner[27] = 1;
    inner[30] = [4];                     // video-capable tool set
    inner[41] = [1];  inner[49] = 11; inner[53] = 0; inner[54] = [];
    inner[55] = [[17]];
    inner[59] = (crypto.randomUUID ? crypto.randomUUID() : '').toUpperCase();
    inner[61] = []; inner[68] = 1; inner[79] = 1; inner[80] = 2;
    inner[91] = 0;  inner[96] = 1;

    reqid += 100000;
    const qs = new URLSearchParams({
      'bl': t.bl || '', 'f.sid': t.sid || '', 'hl': 'en', '_reqid': String(reqid),
    });
    const body = new URLSearchParams({
      'f.req': JSON.stringify([null, JSON.stringify(inner)]),
      'at': t.at,
    }).toString();
    const r = await fetch(STREAM_URL + '?' + qs.toString(), {
      method: 'POST', credentials: 'include',
      headers: {
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'x-same-domain': '1',
      },
      body,
    });
    return await r.text();
  }

  async function fetchBinary(url) {
    const r = await fetch(url, { credentials: 'include' });
    if (!r.ok) throw new Error('download HTTP ' + r.status);
    const buf = new Uint8Array(await r.arrayBuffer());
    let s = '';
    const CH = 0x8000;
    for (let i = 0; i < buf.length; i += CH) {
      s += String.fromCharCode.apply(null, buf.subarray(i, i + CH));
    }
    return btoa(s);
  }

  window.__gvw = { rpc, upload, submit, fetchBinary, tokens };
  return true;
}
"""

UUID_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _install(page):
    page.evaluate(JS_LIB)


def _ensure(page):
    """Re-inject the helpers if the page navigated.

    Submitting through the composer navigates /app -> /app/<conversation>, which
    wipes window.__gvw. Every later rpc() then threw "Cannot read properties of
    undefined (reading 'rpc')" and a finished render was never collected — the
    poll loop ran the full 900s against a video that was actually ready.
    """
    try:
        if page.evaluate("() => !!window.__gvw"):
            return
    except Exception:
        pass
    _install(page)


def _mime_for(path):
    return MIME_BY_EXT.get(os.path.splitext(path)[1].lower(), "application/octet-stream")


def parse_submit(text):
    """(conversation_id, video_job_uuid) out of a StreamGenerate response."""
    conv = re.search(r"(c_[0-9a-f]{12,})", text)
    job = re.search(r"video_gen_chip/\d+.{0,80}?(" + UUID_RE + ")", text, re.S)
    if not job:  # chip index / wrapper can shift — fall back to any uuid near key "65"
        job = re.search(r'\\"65\\".{0,200}?(' + UUID_RE + ")", text, re.S)
    return (conv.group(1) if conv else None), (job.group(1) if job else None)


def parse_download_url(text):
    """The signed mp4 URL out of an hNvQHb conversation payload."""
    # The payload is a JSON string nested inside the batchexecute JSON, so "="
    # arrives as a DOUBLE-escaped \\u003d. Accept 1-4 backslashes, or a raw "=".
    m = re.search(
        r"contribution\.usercontent\.google\.com/download\?c(?:\\{1,4}u003d|=)([A-Za-z0-9_\-]+)",
        text)
    if not m:
        return None
    return ("https://contribution.usercontent.google.com/download"
            f"?c={m.group(1)}&filename=video.mp4")


# Gemini answers a video request with PROSE when it will not or cannot make the
# video. Observed live: "I couldn't do that because I'm getting a lot of requests
# right now. Please try again later." — the worker saw no download URL and kept
# polling the finished conversation for the full 900s timeout.
_REFUSAL_PATTERNS = [
    r"I couldn't do that[^\"\\]{0,160}",
    r"I can't (?:create|generate|make)[^\"\\]{0,160}",
    r"I'm unable to[^\"\\]{0,160}",
    r"getting a lot of requests[^\"\\]{0,120}",
    r"[Pp]lease try again later[^\"\\]{0,60}",
    r"something went wrong[^\"\\]{0,120}",
]
# Transient ones are worth waiting out; the rest are not.
_RATE_LIMIT_HINTS = ("lot of requests", "try again later", "something went wrong")


class GeminiBusy(RuntimeError):
    """Gemini refused for a transient reason (load). Worth retrying."""


class GeminiRefused(RuntimeError):
    """Gemini answered with prose and will not make this video. Permanent."""


def conversation_refusal(text):
    """The refusal sentence Gemini replied with, or None.

    Unescape FIRST: apostrophes arrive as \\' inside the nested JSON, so a
    pattern for "I can't create" never matched the literal "I can\\'t create"
    and a hard refusal read as no-refusal-at-all.
    """
    flat = (text.replace("\\\\n", " ").replace("\\n", " ")
                .replace("\\\\'", "'").replace("\\'", "'")
                .replace('\\\\"', '"').replace('\\"', '"'))
    for pat in _REFUSAL_PATTERNS:
        m = re.search(pat, flat)
        if m:
            return re.sub(r"\s+", " ", m.group(0)).strip()
    return None


def poll_done(text):
    """kwDCne says finished. Pending status ends with one timestamp pair; the
    finished one carries a second pair plus a trailing 1."""
    return bool(re.search(r"\[\d{10},\d+\],\[\d{10},\d+\],1\]", text))


# ---------------------------------------------------------------------------
# high-level steps
# ---------------------------------------------------------------------------

def is_logged_in(page):
    """Signed in? Check SEVERAL signals, not just the token.

    WIZ_global_data.SNlM0e is populated by app JS that has not necessarily run
    yet right after domcontentloaded, so a single check on a freshly launched
    golden reported "logged out" for a session that was perfectly valid and sent
    the operator to a manual-login prompt they did not need.
    """
    try:
        return bool(page.evaluate("""() => {
            try {
                if (window.WIZ_global_data && window.WIZ_global_data.SNlM0e) return true;
            } catch (e) {}
            if (document.querySelector('div.ql-editor[contenteditable=true]')) return true;
            if (document.querySelector("a[aria-label*='Google Account']")) return true;
            if (document.querySelector("[data-test-id='side-nav-sparkle-button']")) return true;
            if (/"SNlM0e":"[^"]+"/.test(document.documentElement.innerHTML)) return true;
            return false;
        }"""))
    except Exception:
        return False


def ensure_logged_in(page, timeout_s=600, session_copied=False):
    """Settle first, prompt only as a last resort.

    When a golden was just copied the session IS signed in; it only needs the
    app to finish booting. Give it real time (and a couple of reloads) before
    telling the operator to do anything — the previous version prompted for a
    manual login immediately after a successful copy.
    """
    page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=60000)
    for attempt in range(1, 7):
        jitter(2.5, 4.0)
        if is_logged_in(page):
            log("already logged into Gemini (copied session)." if session_copied
                else "already logged into Gemini.")
            return True
        if attempt in (3, 5):
            log(f"  page not ready yet (try {attempt}/6) — reloading")
            try:
                page.reload(wait_until="domcontentloaded", timeout=60000)
            except Exception:
                pass
    if session_copied:
        log("copied session did not come up signed in — the account may need a")
        log("one-time Gemini visit in Chrome Beta first. Waiting in the window...")
    log("=" * 60)
    log("  ACTION NEEDED: log into gemini.google.com in the window that opened.")
    log("  ONE-TIME — the session is saved in the worker's own profile.")
    log("=" * 60)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if is_logged_in(page):
            log("logged in — session saved, continuing.")
            return True
        time.sleep(3)
    log("login not completed in time.")
    return False


def quota(page):
    """RAW numbers from the qpEbW usage RPC — semantics UNKNOWN, do not label them.

    Measured 2026-08-03 across three calls on one account:
        HAR capture : [null,5], 2,  8, [1785802995,985455000], 48000, 44000
        live call 1 : [null,5], 2, 16, ...
        live call 2 : [null,5], 2, 24, [1785802995,985455000], 48000, 36461
    Field 1 stayed 2 even after a video was generated, so it is NOT a used-count.
    Field 2 climbed +8 per call, so it is NOT a limit. The 48000 / falling-value
    pair and the fixed timestamp look like a budget + reset epoch, but that is a
    guess and nothing here has been tied to an actual video allowance. Returns the
    raw ints so a caller can log them; never present these as "N of M".
    """
    _install(page)
    txt = page.evaluate("async () => await window.__gvw.rpc('qpEbW', [[[6, 5]]], '/app')")
    m = re.search(r"\[\[\[null,5\],([\d,]+),\[(\d+),\d+\],(\d+),(\d+)\]", txt.replace("\\", ""))
    if not m:
        m2 = re.search(r"\[\[\[null,5\],([\d,]+)", txt.replace("\\", ""))
        return {"raw": m2.group(1)} if m2 else None
    return {"raw": m.group(1), "reset_epoch": int(m.group(2)),
            "total": int(m.group(3)), "remaining": int(m.group(4))}


def upload_ref(page, path):
    b64 = base64.b64encode(open(path, "rb").read()).decode()
    name = os.path.basename(path)
    mime = _mime_for(path)
    log(f"uploading start frame {name} ({len(b64) * 3 // 4} bytes, {mime})")
    cid = page.evaluate(
        "async ([n, b, m]) => await window.__gvw.upload(n, b, m)", [name, b64, mime])
    log(f"  -> {cid[:60]}…")
    return {"id": cid, "name": name, "mime": mime}


def _dump_debug(txt, tag):
    """Persist a full StreamGenerate response so a routing failure is diagnosable.
    The error string alone truncates, and these responses are ~12 KB."""
    d = os.path.join(BASE_DIR, ".gemini_debug")
    try:
        os.makedirs(d, exist_ok=True)
        p = os.path.join(d, f"{tag}_{int(time.time())}.txt")
        with open(p, "w", encoding="utf-8") as f:
            f.write(txt)
        return p
    except OSError:
        return None


def _assistant_text(txt):
    """Any prose Gemini replied with — when it answers instead of generating,
    the reason is in here (refusal, clarifying question, 'I can't do that')."""
    out = []
    for m in re.finditer(r'\\"([^"\\]{40,400}?)\\n?\\"', txt):
        s = m.group(1)
        if " " in s and not s.startswith(("http", "/contrib", "rc_", "c_", "r_")):
            out.append(s)
    return out[:4]


def submit_api(page, prompt, refs):
    refs = [r for r in (refs or []) if r]
    log(f"submitting prompt ({len(refs)} frame ref(s)): {prompt!r}")
    txt = page.evaluate("async ([p, r]) => await window.__gvw.submit(p, r)", [prompt, refs])
    conv, job = parse_submit(txt)
    if not job:
        path = _dump_debug(txt, "submit_no_video_job")
        tools = sorted(set(re.findall(r'\\"(\w+_tool)\\"', txt)))
        # Gemini can end the stream with a server-side error instead of a
        # refusal in prose. Observed 2026-08-03: a long technical Veo prompt
        # produced BardErrorInfo 1155 after two data_analysis_tool ticks, while
        # a short conversational prompt on the SAME image generated fine.
        err = re.search(r"BardErrorInfo\\?\",\[(\d+)\]", txt)
        said = _assistant_text(txt)
        if err:
            log(f"  ! Gemini returned BardErrorInfo {err.group(1)} — stream ended "
                f"early, no video started (tools_invoked={tools or 'none'})")
        else:
            log(f"  ! no video job. tools_invoked={tools or 'none'}")
        for s in said:
            log(f"  ! gemini said: {s[:200]}")
        if path:
            log(f"  ! full response dumped to {path}")
        raise RuntimeError(
            (f"Gemini BardErrorInfo {err.group(1)}" if err else "no video job")
            + f" in StreamGenerate response (tools={tools or 'none'}); "
            f"full response at {path}")
    log(f"  conversation={conv} job={job}")
    return conv, job


def submit_manual(page, timeout_s=600):
    """Operator drives the UI; worker sniffs StreamGenerate off the wire.
    No selectors -> survives any Gemini UI change."""
    caught = {}
    log_path = os.path.join(BASE_DIR, ".gemini_debug", "ui_traffic.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    seen_rpc = set()

    def _note(line):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def on_request(req):
        # Record EVERY Gemini POST, not just StreamGenerate. The "Videos" / Omni
        # composer (Flash Extended, with its own 9:16 control) does NOT go through
        # StreamGenerate — a manual capture filtered to that one URL caught
        # nothing and hung. We do not yet know which RPC it uses, so log them all
        # and let the dump tell us instead of guessing again.
        u = req.url
        if req.method != "POST" or "google.com" not in u:
            return
        if not any(k in u for k in ("BardChatUi", "assistant.lamda", "/upload/",
                                    "batchexecute", "StreamGenerate")):
            return
        rpc = ""
        m = re.search(r"rpcids=([^&]+)", u)
        if m:
            rpc = m.group(1)
        try:
            body = req.post_data or ""
        except Exception:
            body = ""
        key = rpc or u.split("?")[0].split("/")[-1]
        if key not in seen_rpc:
            seen_rpc.add(key)
            p = _dump_debug(f"URL: {u}\n\nBODY:\n{body}", f"ui_req_{key}")
            log(f"  captured UI request {key} -> {p}")
        _note(f"{time.strftime('%H:%M:%S')} POST {key:<14} len={len(body):<7} {u[:160]}")

    def on_response(resp):
        if "job" in caught:
            return
        u = resp.url
        if not any(k in u for k in ("StreamGenerate", "batchexecute")):
            return
        try:
            body = resp.text()
        except Exception:
            return
        conv, job = parse_submit(body)
        if job:
            caught["conv"], caught["job"] = conv, job
            _dump_debug(body, "ui_resp_with_job")
            log(f"  video job seen on {u.split('?')[0].split('/')[-1]}")

    page.on("request", on_request)
    page.on("response", on_response)
    log(f"  traffic log: {log_path}")
    log("=" * 60)
    log("  MANUAL MODE: in the window, attach your start frame, type the prompt,")
    log("  and press send. The worker takes over as soon as the job starts.")
    log("=" * 60)
    deadline = time.time() + timeout_s
    while time.time() < deadline and "job" not in caught:
        time.sleep(1)
    page.remove_listener("request", on_request)
    page.remove_listener("response", on_response)
    if "job" not in caught:
        raise RuntimeError("no video job seen — nothing was submitted in time")
    log(f"  caught conversation={caught['conv']} job={caught['job']}")
    return caught["conv"], caught["job"]


def _recent_conversation_ids(page, limit=6):
    """Newest conversation ids. The Recents list is collapsed by default, which
    leaves zero /app/<id> hrefs in the DOM, so expand it first."""
    try:
        for label in ("Open sidebar", "Toggle Recents"):
            b = page.locator(f"button[aria-label='{label}']").first
            if b.count():
                b.click()
                page.wait_for_timeout(1500)
        return page.evaluate("""() => {
            const s = [];
            document.querySelectorAll('a[href*="/app/"]').forEach(a => {
                const m = a.getAttribute('href').match(/\\/app\\/([0-9a-f]{16})/);
                if (m && !s.includes(m[1])) s.push(m[1]);
            });
            return s;
        }""")[:limit] or []
    except Exception:
        return []


def _sweep_recent_for_video(page, exclude=None, only_new_since=None):
    """(conversation, download_url) for the newest conversation holding a video.

    only_new_since: set of conversation ids that already existed before this
    submit. Anything in it is skipped — without this guard the sweep happily
    returns an OLDER render and the caller reports a success it did not produce.
    """
    for cid in _recent_conversation_ids(page):
        conv = "c_" + cid
        if conv == exclude:
            continue
        if only_new_since is not None and cid in only_new_since:
            continue
        try:
            _ensure(page)
            txt = page.evaluate(
                "async ([c, s]) => await window.__gvw.rpc("
                "'hNvQHb', [c, 10, null, 1, [0], [4], null, 1], s)", [conv, "/app/" + cid])
            url = parse_download_url(txt)
            if url:
                return conv, url
        except Exception:
            continue
    return None


def watch_for_download_url(page):
    """Catch the signed mp4 URL off the network.

    A conversation created seconds ago answers hNvQHb with ~195 chars (empty)
    even while its render completes, so RPC polling alone can miss it entirely.
    The page itself requests the finished video from
    contribution.usercontent.google.com — grabbing that request is direct
    evidence the render is done, and it needs no conversation lookup.
    """
    box = {}

    def on_req(req):
        if "contribution.usercontent.google.com/download" in req.url and "url" not in box:
            box["url"] = req.url
    page.on("request", on_req)
    return box, on_req


def wait_for_video(page, conv, job, timeout_s=GEN_TIMEOUT_S, known_before=None,
                   net_box=None):
    """Poll kwDCne for progress, hNvQHb for the truth (the signed mp4 URL)."""
    src = f"/app/{conv[2:]}" if conv and conv.startswith("c_") else "/app"
    deadline = time.time() + timeout_s
    t0 = time.time()
    n = 0
    log(f"  polling conversation {conv} (job={job or 'none'}) up to {timeout_s}s")
    while time.time() < deadline:
        n += 1
        done = False
        if net_box and net_box.get("url"):
            log(f"  video ready after {int(time.time() - t0)}s (seen on the network)")
            return net_box["url"]
        _ensure(page)           # the composer navigation drops window.__gvw
        try:
            if job:
                st = page.evaluate(
                    "async ([j, s]) => await window.__gvw.rpc('kwDCne', [j], s)", [job, src])
                done = poll_done(st)
        except Exception as e:
            log(f"  poll error (continuing): {e}")
        # every 3rd tick — and always once the poll says done — ask for the asset
        if done or n % 3 == 0:
            try:
                conv_txt = page.evaluate(
                    "async ([c, s]) => await window.__gvw.rpc("
                    "'hNvQHb', [c, 10, null, 1, [0], [4], null, 1], s)", [conv, src])
                url = parse_download_url(conv_txt)
                log(f"  fetch: payload={len(conv_txt)} chars, "
                    f"video_url={'YES' if url else 'no'}")
                if url:
                    log(f"  video ready after {int(time.time() - t0)}s")
                    return url
                # No video AND a prose reply = it already answered and said no.
                # Stop instead of polling a finished conversation to timeout.
                refusal = conversation_refusal(conv_txt)
                if refusal:
                    log(f"  ! Gemini replied instead of rendering: {refusal!r}")
                    if any(h in refusal.lower() for h in _RATE_LIMIT_HINTS):
                        raise GeminiBusy(refusal)
                    raise GeminiRefused(refusal)
                # A finished render has been observed sitting in the conversation
                # while the watched id yielded nothing. If the target keeps coming
                # back empty, sweep the newest conversations for the asset rather
                # than time out on a video that already exists.
                if n >= 9 and n % 6 == 0 and known_before is not None:
                    # Only ever consider conversations created AFTER this submit.
                    # An unrestricted sweep returned the PREVIOUS run's render
                    # (byte-identical file) and reported success — silently
                    # wrong, which is worse than timing out.
                    alt = _sweep_recent_for_video(page, exclude=conv,
                                                  only_new_since=known_before)
                    if alt:
                        log(f"  found the render in {alt[0]} instead of {conv}")
                        return alt[1]
            except (GeminiBusy, GeminiRefused):
                # MUST escape this handler. These were raised inside the try, so
                # the generic `except Exception` below swallowed them and logged
                # "conversation fetch error (continuing)" — the detector fired
                # correctly 14 times on one clip and the loop ignored itself
                # every time, polling a refused conversation to the timeout.
                raise
            except Exception as e:
                log(f"  conversation fetch error (continuing): {e}")
        log(f"  … generating ({int(time.time() - t0)}s){' [job reports done]' if done else ''}")
        time.sleep(POLL_EVERY_S)
    raise TimeoutError(f"video not ready after {timeout_s}s")


def download(page, url, out_path):
    _ensure(page)
    b64 = page.evaluate("async (u) => await window.__gvw.fetchBinary(u)", url)
    data = base64.b64decode(b64)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(data)
    log(f"  saved {out_path} ({len(data)} bytes)")
    if not data[4:8] == b"ftyp":
        log("  WARNING: file does not start with an mp4 'ftyp' box — inspect it.")
    return out_path


END_FRAME_INSTRUCTION = (
    " The first image is the opening frame and the second image is the closing "
    "frame; animate from the first to the second.")

# ---------------------------------------------------------------------------
# UI-driven submit.
#
# WHY NOT synthesize the StreamGenerate POST: slot 3 of that request carries a
# ~2 KB '!'-prefixed context token the app mints in memory. It is NOT in
# WIZ_global_data, NOT in the document HTML and NOT a window global (all three
# checked, 2026-08-04). Sending "" there returns BardErrorInfo 1155 — it worked
# exactly once on a brand-new profile and never again. Driving the composer lets
# the page mint the token itself, the same reason flow_worker.py drives Flow's UI.
#
# Selectors below were read off the live DOM, not guessed.
# ---------------------------------------------------------------------------

SEL = {
    "tools": "button[aria-label='Upload & tools']",
    "editor": "div.ql-editor[contenteditable=true]",
    "aspect": "button[aria-label^='Aspect ratio']",
}


def _click_text(page, needles, tag, timeout_ms=6000):
    """Click the first visible element matching any needle.

    Must include bare div/span: the aspect-ratio options are plain <div>s, not
    menuitems. An earlier selector of only [role=menuitem]/button/[role=option]
    silently matched nothing and the aspect stayed on Landscape.
    """
    sel = ("[role=menuitem]:has-text('{n}'), button:has-text('{n}'), "
           "[role=option]:has-text('{n}'), gem-menu-item:has-text('{n}'), "
           "div:has-text('{n}'), span:has-text('{n}')")
    for n in needles:
        try:
            loc = page.locator(sel.format(n=n)).last   # innermost match
            if loc.count() and loc.is_visible():
                _human_click(page, loc, f"{tag}:{n}")
                page.wait_for_timeout(int(random.uniform(700, 1400)))
                return n
        except Exception:
            continue
    return None


def _human_click(page, locator, label="", settle=(0.4, 0.9)):
    """Click the way a person does: travel the cursor there, pause, press.

    Playwright's locator.click() teleports the pointer and fires instantly, with
    no mousemove trail. Gemini accepted such submits client-side (the composer
    cleared) and then produced an empty conversation, which is what a silently
    dropped automated request looks like. This moves in steps with jitter, waits
    a beat, and presses with a human-length hold.
    """
    box = locator.bounding_box()
    if not box:
        locator.click()
        return
    tx = box["x"] + box["width"] / 2 + random.uniform(-box["width"] / 5, box["width"] / 5)
    ty = box["y"] + box["height"] / 2 + random.uniform(-box["height"] / 5, box["height"] / 5)
    page.mouse.move(tx + random.uniform(-140, 140), ty + random.uniform(-110, 110),
                    steps=random.randint(6, 12))
    page.wait_for_timeout(int(random.uniform(90, 220)))
    page.mouse.move(tx, ty, steps=random.randint(8, 18))
    page.wait_for_timeout(int(random.uniform(*[s * 1000 for s in settle])))
    page.mouse.down()
    page.wait_for_timeout(int(random.uniform(45, 110)))
    page.mouse.up()
    if label:
        log(f"  clicked {label} (human)")


def _human_type(page, text):
    """Type with per-character variance and the odd longer pause, instead of a
    metronome-perfect delay. Kept as the fallback for _enter_prompt."""
    for ch in text:
        page.keyboard.type(ch)
        d = random.uniform(0.03, 0.13)
        if random.random() < 0.06:
            d += random.uniform(0.15, 0.45)
        time.sleep(d)


def _editor_text(page):
    try:
        return (page.evaluate("() => { const e = document.querySelector("
                              "'div.ql-editor[contenteditable=true]'); "
                              "return e ? e.innerText.trim() : ''; }") or "")
    except Exception:
        return ""


def _enter_prompt(page, text):
    """PASTE the prompt rather than typing it character by character.

    Job prompts run 500-900 chars; typing them with human variance cost 30-90s
    per clip. Pasting is what a person actually does with a prompt that long,
    and it is a real Ctrl+V with real key events — the mouse behaviour is what
    defeated the silent server-side drop, not the typing cadence.

    Falls back to character typing if the paste does not land, because Quill has
    already shown it can ignore synthetic input (locator.type() left text in the
    DOM while Angular's model stayed empty and Send rendered disabled).
    """
    want = min(10, len(text))

    def _landed():
        return len(_editor_text(page)) >= want

    def _clear():
        """Drop any partial paste so a retry cannot double the prompt."""
        try:
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            page.wait_for_timeout(250)
        except Exception:
            pass

    # 1) Real clipboard paste, RETRIED. A single miss used to drop straight to
    #    character typing, which costs 30-90s on a 500-1800 char job prompt.
    for attempt in range(1, PASTE_RETRIES + 1):
        try:
            _focus_editor(page)
            page.evaluate("async (t) => { await navigator.clipboard.writeText(t); }", text)
            page.wait_for_timeout(int(random.uniform(200, 450)))
            page.keyboard.press("Control+V")
            page.wait_for_timeout(int(random.uniform(700, 1200)))
            if _landed():
                log(f"  pasted {len(text)} chars"
                    + (f" (attempt {attempt})" if attempt > 1 else ""))
                return True
            log(f"  paste attempt {attempt}/{PASTE_RETRIES} did not land "
                f"(editor has {len(_editor_text(page))} chars)")
        except Exception as e:
            log(f"  paste attempt {attempt}/{PASTE_RETRIES} errored "
                f"({e.__class__.__name__})")
        _clear()
        page.wait_for_timeout(int(random.uniform(400, 900)))

    # 2) insert_text: one synthetic input event, still instant. Worth trying
    #    before typing because it is ~1000x faster on a long prompt.
    try:
        _focus_editor(page)
        page.keyboard.insert_text(text)
        page.wait_for_timeout(800)
        if _landed():
            log(f"  inserted {len(text)} chars (clipboard paste failed)")
            return True
        _clear()
    except Exception as e:
        log(f"  insert_text failed ({e.__class__.__name__})")

    # 3) Last resort. Slow, but Quill has shown it can ignore synthetic input.
    log(f"  falling back to character typing ({len(text)} chars — this is slow)")
    _human_type(page, text)
    return False


def _dump_composer_state(page, tag):
    """Record what the composer actually offers. Called when an expected control
    is missing, so the next failure is diagnosed from evidence."""
    try:
        info = page.evaluate("""() => {
            const btns = [];
            document.querySelectorAll('button,[role=menuitem],[role=option]').forEach(b => {
                const t = (b.getAttribute('aria-label') || b.innerText || '').trim();
                if (t) btns.push(t.slice(0, 60));
            });
            return {url: location.href, buttons: btns.slice(0, 60),
                    hasEditor: !!document.querySelector('div.ql-editor[contenteditable=true]'),
                    overlay: !!document.querySelector('.cdk-overlay-backdrop-showing')};
        }""")
        log(f"  [{tag}] url={info['url']}")
        log(f"  [{tag}] editor={info['hasEditor']} overlay={info['overlay']}")
        log(f"  [{tag}] controls: {info['buttons']}")
        p = _dump_debug(repr(info), f"composer_{tag}")
        try:
            page.screenshot(path=os.path.join(BASE_DIR, ".gemini_debug",
                                              f"composer_{tag}_{int(time.time())}.png"))
        except Exception:
            pass
        return info
    except Exception as e:
        log(f"  [{tag}] state dump failed: {e}")
        return {}


def _ensure_chat_mode(page):
    """Force the CHAT surface, never Spark.

    Gemini has a Chat|Spark toggle and a "Switch to Spark (Ctrl+Shift+S)"
    shortcut. Once Spark is active there is no "Upload & tools" button, so every
    later step dies — observed as two filechooser timeouts followed by
    Locator.bounding_box timing out on a button that does not exist in Spark.
    Cheap to check, fatal to miss.
    """
    try:
        if page.locator(SEL["tools"]).count():
            return True                      # composer is present -> chat surface
        chat = page.locator(
            "button:has-text('Chat'), [role=tab]:has-text('Chat')").first
        if chat.count() and chat.is_visible():
            log("  ! Spark surface detected — switching back to Chat")
            _human_click(page, chat, "Chat tab")
            page.wait_for_timeout(2500)
            return bool(page.locator(SEL["tools"]).count())
    except Exception as e:
        log(f"  (chat-mode check failed: {e.__class__.__name__})")
    return False


def _focus_editor(page):
    """Click the composer and PROVE it took focus.

    Keystrokes that land on the document instead of the editor are how a stray
    app shortcut gets triggered (Spark is Ctrl+Shift+S, and typed capitals send
    Shift). Never type until the editor is really focused.
    """
    ed = page.locator(SEL["editor"]).first
    for attempt in range(3):
        try:
            _human_click(page, ed, "prompt box" if attempt == 0 else "prompt box (refocus)")
        except Exception:
            _dismiss_overlay(page)
            try:
                ed.click(force=True)
            except Exception:
                pass
        page.wait_for_timeout(int(random.uniform(350, 700)))
        try:
            if page.evaluate("() => { const a = document.activeElement; "
                             "return !!(a && a.classList && "
                             "a.classList.contains('ql-editor')); }"):
                return ed
        except Exception:
            pass
        log(f"  editor not focused (try {attempt + 1}/3)")
        _dismiss_overlay(page)
    return ed


def _dismiss_overlay(page):
    """Angular Material leaves a `cdk-overlay-backdrop` behind after a menu
    selection; it swallows pointer events and makes the composer unclickable
    ("subtree intercepts pointer events"). Escape closes it."""
    for _ in range(4):
        try:
            back = page.locator(".cdk-overlay-backdrop-showing")
            if not back.count():
                return True
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
        except Exception:
            return False
    return False


def _set_aspect(page, portrait=True):
    """The composer's aspect control defaults to Landscape (16:9). Jobs are 9:16,
    so this must be flipped explicitly — it is a UI control, not prompt wording."""
    try:
        btn = page.locator(SEL["aspect"]).first
        if not btn.count():
            # The aspect control is the proof Videos mode is really on. Missing
            # means the tool did not activate, and every later step will fail.
            log("  ! no aspect control found — Videos mode did NOT activate")
            _dump_composer_state(page, "no_aspect_control")
            return False
        label = btn.get_attribute("aria-label") or ""
        want = "Portrait" if portrait else "Landscape"
        if want.lower() in label.lower():
            log(f"  aspect already {want}")
            return True
        log(f"  aspect control reads: {label!r}")
        btn.click()
        page.wait_for_timeout(1500)
        opts = page.evaluate("""() => {
            const r = [];
            document.querySelectorAll('[role=menuitem],[role=option],button').forEach(b => {
                const t = (b.getAttribute('aria-label') || b.innerText || '').trim();
                if (t && /portrait|landscape|square|9:16|16:9|1:1/i.test(t)) r.push(t.slice(0,40));
            });
            return r;
        }""")
        log(f"  aspect options offered: {opts}")
        hit = _click_text(page, [f"{want} (9:16)" if portrait else f"{want} (16:9)", want],
                          "aspect")
        log(f"  aspect -> {hit or 'NOT SET'}")
        return bool(hit)
    except Exception as e:
        log(f"  ! aspect select failed: {e}")
        return False


def submit_ui(page, prompt, ref_paths, portrait=None, timeout_s=180):
    """portrait=None SKIPS the aspect control on purpose.

    The bare sequence (attach -> Videos tool -> type -> send) is proven: it
    produced conversation c_7d01e2cad898419c and a real render. Adding an aspect
    click on top of it broke the submit — the dropdown does not open from the
    trigger (its options render outside the queried subtree), and the leftover
    overlay left the composer in a state where Send produced no video job.
    Aspect defaults to Landscape 16:9, so 9:16 jobs still need this solved;
    doing it wrong costs a whole render, so it stays off until the real selector
    is known. Set portrait=True only when testing that.
    """
    """Drive the Videos composer: attach frame(s), pick Videos + 9:16, send.
    Returns (conversation_id, video_job_uuid) sniffed off the network."""
    caught = {}

    def on_response(resp):
        if "StreamGenerate" not in resp.url or "job" in caught:
            return
        try:
            body = resp.text()
        except Exception:
            return
        conv, job = parse_submit(body)
        if job:
            caught["conv"], caught["job"] = conv, job
        elif "BardErrorInfo" in body:
            caught["err"] = _dump_debug(body, "ui_submit_error")

    page.on("response", on_response)
    try:
        # Start every clip from a genuinely clean composer. After clip 1 the tab
        # sits inside a conversation; a bare goto('/app') left enough state
        # behind that the SECOND clip's submit created no conversation at all
        # ("submit produced no conversation"). Reload, then explicitly take
        # New chat, then prove the composer is empty and unattached.
        page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(int(random.uniform(6000, 8000)))
        try:
            newchat = page.locator(
                "button:has-text('New chat'), a:has-text('New chat')").first
            if newchat.count() and newchat.is_visible():
                _human_click(page, newchat, "New chat")
                page.wait_for_timeout(int(random.uniform(3000, 4500)))
        except Exception as e:
            log(f"  (New chat not clickable: {e})")

        _ensure_chat_mode(page)

        for _ in range(5):
            leftover = page.locator("button[aria-label='close attachment']")
            if not leftover.count():
                break
            log("  clearing a leftover attachment from the previous clip")
            try:
                leftover.first.click()
                page.wait_for_timeout(1200)
            except Exception:
                break
        stale = _editor_text(page)
        if stale:
            log(f"  clearing {len(stale)} stale chars from the composer")
            try:
                _focus_editor(page)          # never send keys at the document
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.wait_for_timeout(600)
            except Exception:
                pass

        # ORDER MATTERS — this mirrors the operator's own working click sequence
        # (captured 2026-08-04): pick the Videos tool FIRST, then set aspect, then
        # attach through the composer's own "File upload" button. Attaching first
        # via the generic "Upload files" (documents/data/code) entry and selecting
        # Videos afterwards never submitted.
        # Videos mode PERSISTS across New chat. When it is already on, the menu
        # item reads "Deselect Videos" and there is no "Create video" to click —
        # blindly selecting it failed 5 clips in a row with "could not select the
        # Videos tool". The aspect control only exists in Videos mode, so its
        # presence is the reliable probe.
        # Retry the whole probe: on a page that has not finished settling none of
        # the three signals exist yet, and a single-shot check raised
        # "could not select the Videos tool" on an otherwise healthy clip.
        for tool_try in range(1, 4):
            if page.locator(SEL["aspect"]).count():
                log("  Videos mode already active (aspect control present)")
                break
            _human_click(page, page.locator(SEL["tools"]).first, "Upload & tools")
            page.wait_for_timeout(int(random.uniform(1400, 2400)))
            if page.locator("button:has-text('Deselect Videos'), "
                            "[role=menuitem]:has-text('Deselect Videos')").count():
                log("  Videos mode already active (Deselect Videos offered)")
                _dismiss_overlay(page)
                break
            if _click_text(page, ["Create video"], "videos-tool"):
                # VERIFY BY OUTCOME, not by the click landing. The aspect control
                # only exists in Videos mode, so wait for it to appear. Breaking
                # on a successful click alone is why a whole redo batch died with
                # "no aspect control" then "no file-upload control" — the click
                # reported success while the tool had not actually activated.
                try:
                    page.wait_for_selector(SEL["aspect"], timeout=12000)
                    log("  Videos mode confirmed (aspect control appeared)")
                    break
                except Exception:
                    log(f"  clicked the Videos tool but it did not activate "
                        f"(try {tool_try}/3)")
                    _dismiss_overlay(page)
                    continue
            log(f"  Videos tool not offered yet (try {tool_try}/3) — settling")
            _dismiss_overlay(page)
            # A missing tools menu usually means the surface flipped to Spark.
            if not _ensure_chat_mode(page):
                page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
                _ensure_chat_mode(page)
            page.wait_for_timeout(4000)
        else:
            raise RuntimeError("could not select the Videos tool after 3 tries")
        page.wait_for_timeout(int(random.uniform(1600, 2600)))

        if portrait is not None:
            _set_aspect(page, portrait=portrait)
            _dismiss_overlay(page)

        for path in (ref_paths or []):
            _dismiss_overlay(page)
            btn = page.locator("button[aria-label='File upload']").first
            if not btn.count():
                # Fall back to the tools menu, but only when the composer's own
                # upload button really is absent. Opening the menu first when the
                # button existed cost two clips to filechooser timeouts.
                _human_click(page, page.locator(SEL["tools"]).first, "Upload & tools")
                page.wait_for_timeout(int(random.uniform(1000, 1800)))
            with page.expect_file_chooser(timeout=25000) as fc:
                if btn.count():
                    _human_click(page, btn, "File upload")
                elif not _click_text(page, ["Upload files", "Upload"], "upload"):
                    _dump_composer_state(page, "no_upload_control")
                    raise RuntimeError("no file-upload control found")
            fc.value.set_files(path)
            # Wait for the attachment to finish, not a fixed sleep: the composer
            # grows a "close attachment" control once the file is really staged.
            # Sending before that leaves Send enabled but inert.
            staged = False
            for _ in range(40):
                page.wait_for_timeout(1000)
                if page.locator("button[aria-label='close attachment']").count():
                    staged = True
                    break
            log(f"  attached {os.path.basename(path)} (staged={staged})")

        _dismiss_overlay(page)

        ed = _focus_editor(page)
        # The composer treats Enter as send, so a multi-line prompt is flattened.
        flat = prompt.replace("\n\n", " ").replace("\n", " ")
        # page.keyboard.type dispatches real key events; locator.type() left the
        # text in the DOM while Angular's model stayed empty, so Send rendered
        # disabled and the prompt was never sent.
        _enter_prompt(page, flat)
        page.wait_for_timeout(int(random.uniform(900, 1800)))
        # VERIFY the text actually landed. Quill can swallow synthetic input, and
        # then Send is a no-op and the page never navigates — which is exactly the
        # failure that kept showing up as "submit produced no conversation".
        got = _editor_text(page)
        if len(got) < min(10, len(flat)):
            log(f"  ! editor text did not land (saw {got!r}) — retrying via insert_text")
            ed.click()
            page.keyboard.insert_text(flat)
            page.wait_for_timeout(800)
            got = _editor_text(page)
        log(f"  editor holds {len(got)} chars: {got[:70]!r}")
        if not got:
            raise RuntimeError("could not type the prompt into the composer")
        # Prefer the real Send button: Escape (used to clear the menu backdrop)
        # can pull focus off the editor, and then Enter goes nowhere.
        # Wait for Send to actually enable — it only does so once the framework
        # has registered BOTH the typed prompt and the staged attachment. Sending
        # (or Enter-ing) before that is a silent no-op.
        send = page.locator("button[aria-label='Send message']").first
        for attempt in range(30):
            if send.count() and send.is_enabled():
                break
            if attempt == 10:
                log("  Send still disabled — nudging the editor")
                try:
                    ed.click()
                    page.keyboard.type(" ")
                    page.keyboard.press("Backspace")
                except Exception:
                    pass
            page.wait_for_timeout(1000)
        # The attachment must still be staged at send time. Symptom when it is
        # not: Send "works", the conversation is created and titled from the
        # prompt, the page navigates — then the prompt bounces back into the
        # composer and the conversation stays empty (hNvQHb returns ~195 chars).
        att = page.locator("button[aria-label='close attachment']").count()
        log(f"  attachment chips present at send time: {att}")
        if ref_paths and not att:
            log("  ! attachment was LOST before send — re-attaching")
            for path in ref_paths:
                try:
                    with page.expect_file_chooser(timeout=20000) as fc:
                        page.locator("button[aria-label='File upload']").first.click()
                    fc.value.set_files(path)
                    for _ in range(40):
                        page.wait_for_timeout(1000)
                        if page.locator("button[aria-label='close attachment']").count():
                            break
                except Exception as e:
                    log(f"  ! re-attach failed: {e}")
            log(f"  attachment chips after re-attach: "
                f"{page.locator('button[aria-label=close attachment]').count()}")
        if send.count():
            log(f"  Send button present, enabled={send.is_enabled()}")
        if send.count() and send.is_enabled():
            # The send is FLAKY: identical code has produced a real render and,
            # other times, created an empty titled conversation while the prompt
            # bounced back into the composer. A bounced prompt leaves text in the
            # editor, so retry on that signal. Failed submits cost no render
            # quota, which makes retrying the cheap option.
            for attempt in range(1, 4):
                _human_click(page, send, f"Send (attempt {attempt})")
                page.wait_for_timeout(8000)
                left = (page.evaluate("() => { const e = document.querySelector("
                                      "'div.ql-editor[contenteditable=true]'); "
                                      "return e ? e.innerText.trim() : ''; }") or "")
                if not left:
                    log("  composer cleared — prompt accepted")
                    break
                log(f"  prompt bounced back into the composer ({len(left)} chars) — retrying")
                send = page.locator("button[aria-label='Send message']").first
                if not (send.count() and send.is_enabled()):
                    ed.click()
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(4000)
                    break
            page.wait_for_timeout(2000)
            # NEVER re-send on a slow navigation. The composer CLEARS the moment
            # a prompt is accepted, so an empty editor means Send worked even if
            # the URL has not flipped yet. Retrying on the URL alone fired a
            # SECOND prompt into a render already in flight — the operator saw a
            # duplicate generation and the "Create videos" page reappear.
            still = (page.evaluate("() => { const e = document.querySelector("
                                   "'div.ql-editor[contenteditable=true]'); "
                                   "return e ? e.innerText.trim() : ''; }") or "")
            navigated = bool(re.search(r"/app/([0-9a-f]{16})", page.url))
            if not navigated and still:
                log("  composer still holds the prompt — pressing Enter once")
                try:
                    ed.click()
                    page.keyboard.press("Enter")
                except Exception:
                    pass
            elif not navigated:
                log("  composer cleared — prompt accepted, waiting for navigation")
        else:
            page.keyboard.press("Enter")
            log("  pressed Enter (Send unavailable)")

        # Identify the conversation from the URL, not from a parsed response.
        # Response sniffing proved unreliable here (streaming bodies), while the
        # URL flip to /app/<hex16> is what the working drive relied on. The job
        # uuid is a bonus if the sniffer happens to catch it; the download only
        # needs the conversation.
        deadline = time.time() + timeout_s
        conv = None
        while time.time() < deadline:
            if "err" in caught:
                raise RuntimeError(f"Gemini rejected the submit; dump at {caught['err']}")
            m = re.search(r"/app/([0-9a-f]{16})", page.url)
            if m:
                conv = "c_" + m.group(1)
                break
            if "conv" in caught and caught["conv"]:
                conv = caught["conv"]
                break
            time.sleep(1)
        if not conv:
            _dump_debug(page.url + "\n\n" + page.content()[:200000], "ui_submit_no_conv")
            try:
                shot = os.path.join(BASE_DIR, ".gemini_debug",
                                    f"ui_submit_no_conv_{int(time.time())}.png")
                page.screenshot(path=shot, full_page=False)
                log(f"  ! screenshot of the stuck composer: {shot}")
            except Exception:
                pass
            raise RuntimeError("submit produced no conversation (page never navigated)")
        job = caught.get("job")
        log(f"  conversation={conv} job={job or '(not sniffed — will poll by conversation)'}")
        return conv, job
    finally:
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass


def generate(page, prompt=None, ref_path=None, out_path="out.mp4", manual=False,
             end_frame_path=None):
    _install(page)
    if manual:
        conv, job = submit_manual(page)
    else:
        paths = [p for p in (ref_path, end_frame_path) if p]
        if end_frame_path:
            prompt = (prompt or "") + END_FRAME_INSTRUCTION
        # Snapshot the existing conversations BEFORE submitting so the recovery
        # sweep can never hand back a render from an earlier run.
        before = set(_recent_conversation_ids(page, limit=25))
        net_box, net_handler = watch_for_download_url(page)
        # Jobs are 9:16; the composer defaults to Landscape 16:9, so ask for
        # Portrait explicitly. The option is a plain <div> — see _click_text.
        conv, job = submit_ui(page, prompt, paths, portrait=True)
    if manual:
        before, net_box, net_handler = None, None, None
    url = wait_for_video(page, conv, job, known_before=before, net_box=net_box)
    if net_handler:
        try:
            page.remove_listener("request", net_handler)
        except Exception:
            pass
    _install(page)   # composer navigation drops the injected helpers
    return download(page, url, out_path)


# ---------------------------------------------------------------------------
# HTTP-pull mode — claims the SAME queue flow_worker.py claims
# (/api/user-worker, Job.backend == 'flow'). Emergency stand-in for Flow.
# ---------------------------------------------------------------------------

WEB_APP_URL = os.environ.get("WEB_APP_URL", "https://kavenobuilder.com")
API_PATH_PREFIX = "/api/user-worker"
WORKER_ID = os.environ.get("WORKER_ID", f"gemini-emergency-{uuid.uuid4().hex[:8]}")


class Api:
    """Thin client for the user-worker API. Same endpoints, same auth header,
    same payload shapes flow_worker.py uses."""

    def __init__(self, base, token):
        import requests
        self.requests = requests
        self.base = base.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}

    def _url(self, ep):
        return f"{self.base}{API_PATH_PREFIX}{ep}"

    def get(self, ep):
        r = self.requests.get(self._url(ep), headers=self.headers, timeout=30)
        return (r.json() if r.status_code == 200 else None), r.status_code

    def post(self, ep, data):
        r = self.requests.post(self._url(ep), headers=self.headers, json=data, timeout=30)
        return (r.json() if r.status_code == 200 else None), r.status_code

    def pending_job(self, want_id=None, max_walk=25):
        """Next claimable job, or specifically `want_id`.

        /jobs/{id} is status-only (no prompts, no frame URLs), so a targeted run
        still has to come through /jobs/pending — which serves the OLDEST job
        first. Walk past the others with `exclude` until the wanted id shows up.
        """
        res, _ = self.get(f"/jobs/pending?worker_id={WORKER_ID}")
        job = (res or {}).get("job")
        if not want_id or not job or job.get("id") == want_id:
            return job
        seen = [job["id"]]
        for _ in range(max_walk):
            res, _ = self.get(f"/jobs/pending?worker_id={WORKER_ID}"
                              f"&exclude={','.join(seen)}")
            job = (res or {}).get("job")
            if not job:
                log(f"walked {len(seen)} job(s); {want_id[:8]}… was not among them")
                return None
            if job.get("id") == want_id:
                log(f"skipped {len(seen)} older job(s) to reach {want_id[:8]}…")
                return job
            seen.append(job["id"])
        return None

    def redo_clips(self):
        """Clips queued for regeneration — the retry/redo lane flow_worker.py
        claims. Same endpoint, same claim semantics (claiming is done server
        side when worker_id is supplied)."""
        res, _ = self.get(f"/clips/redo-pending?worker_id={WORKER_ID}")
        return (res or {}).get("clips") or []

    def job_status(self, job_id, status, error=None):
        return self.post(f"/jobs/{job_id}/status",
                         {"status": status, "error_message": error})

    def clip_status(self, clip_id, status, output_url=None, error=None):
        return self.post(f"/clips/{clip_id}/status",
                         {"status": status, "output_url": output_url,
                          "error_message": error})

    def fetch_frame(self, url, dest):
        r = self.requests.get(url, headers=self.headers, timeout=120)
        r.raise_for_status()
        with open(dest, "wb") as f:
            f.write(r.content)
        return dest

    def upload_video(self, path, job_id, clip_index, attempt=1, variant=1):
        # Filename convention is load-bearing on the server side:
        # clip_{index}_{attempt}.{variant}.mp4  (see flow_worker.upload_video)
        name = f"clip_{clip_index}_{attempt}.{variant}.mp4"
        with open(path, "rb") as f:
            r = self.requests.post(
                self._url(f"/jobs/{job_id}/upload-video/{clip_index}"),
                headers=self.headers, files={"file": (name, f, "video/mp4")},
                timeout=300)
        r.raise_for_status()
        return (r.json() or {}).get("url")


DURATION_TOLERANCE_S = float(os.environ.get("GEMINI_DURATION_TOLERANCE_S", "0.5"))
BUSY_RETRIES = int(os.environ.get("GEMINI_BUSY_RETRIES", "3"))
UI_RETRIES = int(os.environ.get("GEMINI_UI_RETRIES", "2"))
PASTE_RETRIES = int(os.environ.get("GEMINI_PASTE_RETRIES", "3"))
BUSY_BACKOFF_S = int(os.environ.get("GEMINI_BUSY_BACKOFF_S", "90"))


def _probe_duration(path):
    """Seconds of `path` per ffprobe, or None when ffprobe is unavailable."""
    import subprocess
    exe = os.environ.get("FFPROBE_BIN", "ffprobe")
    try:
        out = subprocess.run(
            [exe, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", path],
            capture_output=True, text=True, timeout=60).stdout.strip()
        return float(out.splitlines()[0]) if out else None
    except Exception:
        return None


def _check_duration(path, want):
    """Duration is asked for in PROMPT WORDING, which Gemini honours but does not
    guarantee. Measured on job d8f1b043: 8 of 9 clips landed within 16ms of the
    request and one came back 3.008s against an 8s ask — and it uploaded and was
    marked completed anyway, so a 5-second-short clip shipped looking fine.
    Raise instead, so the clip re-queues rather than silently passing.
    """
    if not want:
        return
    got = _probe_duration(path)
    if got is None:
        log("  ! ffprobe unavailable — duration NOT verified for this clip")
        return
    if abs(got - float(want)) > DURATION_TOLERANCE_S:
        raise RuntimeError(
            f"duration mismatch: asked {want}s, got {got:.3f}s "
            f"(tolerance {DURATION_TOLERANCE_S}s) — not uploading")
    log(f"  duration verified: {got:.3f}s vs {want}s asked")


def _clip_prompt(clip):
    return (clip.get("prompt") or clip.get("dialogue_text") or "").strip()


def _with_duration(prompt, seconds):
    """Duration goes in FRONT of the real prompt, as a plain instruction.

    Wording + blank-line separator copied VERBATIM from an operator render that
    is confirmed to work: the prompt

        generate a video of 4 seconds

        she talks

    produced a 4.010s video (measured with ffprobe on conversation
    c_1b63c07c5f9c87f4, 2026-08-04). Do not "tidy" this into one line or
    sentence-case it without re-measuring — the exact form is the evidence.
    """
    if not seconds:
        return prompt
    return f"generate a video of {seconds} seconds\n\n{prompt}"


def process_clip(page, api, job, clip, work_dir, attempt=1):
    """One clip: pull frames -> generate -> upload -> report. Raises on failure.

    `attempt` lands in the uploaded filename (clip_{i}_{attempt}.{variant}.mp4).
    A redo MUST carry its real generation_attempt or it overwrites the previous
    attempt's file instead of sitting beside it.
    """
    idx = clip["clip_index"]
    clip_id = clip["id"]
    prompt = _clip_prompt(clip)
    if not prompt:
        raise ValueError(f"clip {idx} has no prompt")

    start_path = end_path = None
    if clip.get("start_frame_url"):
        start_path = api.fetch_frame(clip["start_frame_url"],
                                     os.path.join(work_dir, f"c{idx}_start.png"))
    if clip.get("end_frame_url") and clip["end_frame_url"] != clip.get("start_frame_url"):
        end_path = api.fetch_frame(clip["end_frame_url"],
                                   os.path.join(work_dir, f"c{idx}_end.png"))

    want = clip.get("veo_render_duration_s") or job.get("duration")
    prompt = _with_duration(prompt, want)
    log(f"clip {idx + 1}: mode={clip.get('clip_mode')} start={bool(start_path)} "
        f"end={bool(end_path)} requested_duration={want}s")

    api.clip_status(clip_id, "generating")
    out = os.path.join(work_dir, f"clip_{idx}.mp4")
    # "I'm getting a lot of requests right now" is load, not rejection — wait it
    # out rather than failing the clip and marching on into the same wall.
    # NOTE: this counter must NOT be called `attempt` — that is the caller's
    # generation_attempt, and shadowing it made every redo upload as attempt 1,
    # overwriting the previous attempt's file instead of sitting beside it.
    for try_n in range(1, BUSY_RETRIES + 2):
        try:
            generate(page, prompt=prompt, ref_path=start_path, out_path=out,
                     end_frame_path=end_path)
            break
        except GeminiBusy as e:
            if try_n > BUSY_RETRIES:
                raise
            wait = BUSY_BACKOFF_S * try_n
            log(f"  Gemini busy ({e}) — waiting {wait}s, retry {try_n}/{BUSY_RETRIES}")
            time.sleep(wait)
        except GeminiRefused:
            raise                       # permanent: do not waste retries on it
        except Exception as e:
            # Composer/UI hiccups (tool did not activate, upload control missing,
            # submit did not navigate) are transient and cost a whole clip each.
            # Reload and try again rather than failing the clip on first stumble.
            if try_n > UI_RETRIES:
                raise
            log(f"  UI problem ({e.__class__.__name__}: {str(e)[:90]}) — "
                f"reloading and retrying {try_n}/{UI_RETRIES}")
            try:
                page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(5000)
            except Exception:
                pass
    _check_duration(out, want)   # refuse to upload a clip of the wrong length

    url = api.upload_video(out, job["id"], idx, attempt=attempt)
    api.clip_status(clip_id, "completed", output_url=url)
    log(f"clip {idx + 1}: completed (attempt {attempt}) -> {url}")
    return url


def process_redo_clips(page, api, max_clips=None):
    """Drain the retry/redo lane. Returns how many clips were attempted.

    Redo clips carry their own job_id and duration, so each one is processed
    against a minimal job dict rather than a claimed job.
    """
    try:
        clips = api.redo_clips()
    except Exception as e:
        log(f"redo poll error: {e}")
        return 0
    if not clips:
        return 0

    log(f"redo queue: {len(clips)} clip(s) waiting")
    done = 0
    for clip in clips:
        if max_clips is not None and done >= max_clips:
            log(f"  reached --max-clips {max_clips}; leaving {len(clips) - done} redo(s) queued")
            break
        job_id = clip.get("job_id")
        if not job_id:
            log(f"  ! redo clip {clip.get('id')} has no job_id — skipping")
            continue
        job = {"id": job_id, "duration": clip.get("duration"),
               "aspect_ratio": clip.get("aspect_ratio", "9:16")}
        work_dir = os.path.join(BASE_DIR, ".gemini_work", job_id)
        os.makedirs(work_dir, exist_ok=True)
        attempt = int(clip.get("generation_attempt") or 1)
        reason = clip.get("redo_reason") or "requeued"
        log(f"redo clip {clip['clip_index'] + 1} of job {job_id[:8]}… "
            f"(attempt {attempt}, reason: {reason})")
        done += 1
        try:
            process_clip(page, api, job, clip, work_dir, attempt=attempt)
        except GeminiRefused as e:
            log(f"  x redo FAILED (Gemini refused): {e}")
            try:
                api.clip_status(clip["id"], "failed", error=f"Gemini refused: {e}")
            except Exception:
                pass
        except Exception as e:
            log(f"  x redo FAILED: {e}")
            try:
                api.clip_status(clip["id"], "failed", error=str(e))
            except Exception:
                pass
        jitter(3, 7)
    return done


def process_job(page, api, job, max_clips=None):
    clips = job.get("clips") or []
    log(f"job {job['id'][:8]}… — {len(clips)} clip(s), aspect={job.get('aspect_ratio')}")
    api.job_status(job["id"], "processing")

    work_dir = os.path.join(BASE_DIR, ".gemini_work", job["id"])
    os.makedirs(work_dir, exist_ok=True)

    done, failed, rendered = 0, [], 0
    for clip in clips:
        if clip.get("status") == "completed":
            done += 1
            continue
        if max_clips is not None and rendered >= max_clips:
            # Partial run (a deliberate test limit). Hand the job BACK as
            # claimable instead of leaving it 'processing', which no worker
            # picks up — that would strand it. Completed clips keep their
            # uploads, so the main worker resumes from where this stopped.
            log(f"  reached --max-clips {max_clips}; returning the job to the queue")
            api.job_status(job["id"], "pending")
            log(f"job {job['id'][:8]}… PARTIAL — {rendered} clip(s) rendered, "
                f"{len(clips) - done} still pending, job set back to 'pending'")
            return
        rendered += 1        # count ATTEMPTS: counting only successes let a
                             # failing job march past --max-clips unchecked
        try:
            process_clip(page, api, job, clip, work_dir)
            done += 1
        except Exception as e:
            log(f"  x clip {clip['clip_index'] + 1} FAILED: {e}")
            failed.append(clip["clip_index"])
            try:
                api.clip_status(clip["id"], "failed", error=str(e))
            except Exception:
                pass
        jitter(3, 7)

    if failed:
        api.job_status(job["id"], "failed",
                       error=f"gemini emergency worker: clips {failed} failed")
        log(f"job {job['id'][:8]}… FAILED on clips {failed} ({done}/{len(clips)} ok)")
    else:
        api.job_status(job["id"], "completed")
        log(f"job {job['id'][:8]}… COMPLETED ({done}/{len(clips)})")


def http_pull_loop(page, api, poll_s=10, once=False, max_clips=None, job_id=None):
    log(f"emergency worker online — worker_id={WORKER_ID}, polling {api.base}"
        + (f", targeting job {job_id}" if job_id else ""))
    idle = 0
    while True:
        try:
            job = api.pending_job(want_id=job_id)
        except Exception as e:
            log(f"poll error: {e}")
            job = None
        # Retries/redos first — they are work the operator already asked to be
        # re-done, and a failed clip sitting in the redo lane blocks its job from
        # ever completing.
        if not job_id:
            try:
                if process_redo_clips(page, api, max_clips=max_clips):
                    idle = 0
                    if once:
                        return
                    continue
            except Exception as e:
                log(f"redo lane error: {e}")

        if job:
            idle = 0
            try:
                process_job(page, api, job, max_clips=max_clips)
            except Exception as e:
                log(f"job error: {e}")
                try:
                    api.job_status(job["id"], "failed", error=str(e))
                except Exception:
                    pass
            if once:
                return
        else:
            if job_id and once:
                # Targeted single run and the job is not claimable — usually
                # another worker holds it. Say so and exit instead of spinning
                # (this logged "was not among them" 105 times and never quit).
                log(f"job {job_id} is not claimable — another worker may hold it. "
                    f"Stop the other worker, or drop --once to keep waiting.")
                return
            idle += 1
            if idle % 6 == 1:
                log("no pending jobs — waiting")
        time.sleep(poll_s)


# ---------------------------------------------------------------------------

def login_flow(email=None):
    if email:
        use_account(email)
    with _import_playwright()() as p:
        ctx, page = launch(p)
        try:
            page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=60000)
            log("Browser open. Log into Gemini, then press ENTER here.")
            try:
                input()
            except (EOFError, KeyboardInterrupt):
                pass
            log(f"Session saved to {PROFILE_DIR}. logged_in={is_logged_in(page)}")
        finally:
            ctx.close()


def main():
    ap = argparse.ArgumentParser(description="Gemini web video (Veo) worker")
    ap.add_argument("--login", action="store_true", help="headful manual login, save session")
    ap.add_argument("--prompt", help="the video prompt")
    ap.add_argument("--ref", help="start-frame image to animate (optional)")
    ap.add_argument("--out", default="out.mp4", help="output mp4 path")
    ap.add_argument("--manual", action="store_true",
                    help="you submit in the UI; the worker polls + downloads")
    ap.add_argument("--quota", action="store_true", help="print the video quota and exit")
    ap.add_argument("--jobs", help="JSON list of {prompt, ref, out} jobs")
    ap.add_argument("--serve", action="store_true",
                    help="EMERGENCY MODE: claim the same job queue flow_worker.py "
                         "claims and render the clips through Gemini")
    ap.add_argument("--api-url", default=WEB_APP_URL, help="platform base URL")
    ap.add_argument("--token", default=os.environ.get("USER_WORKER_TOKEN", ""),
                    help="worker token (or set USER_WORKER_TOKEN)")
    ap.add_argument("--once", action="store_true", help="--serve: one job, then exit")
    ap.add_argument("--job-id", help="--serve: render THIS job id specifically")
    ap.add_argument("--max-clips", type=int, default=None,
                    help="--serve: render at most N clips, then return the job to "
                         "the queue for the main worker (test guard)")
    ap.add_argument("--email", help="Google account to run as. Its live session is "
                    "COPIED from a non-stable Chrome channel (Beta/Dev/Canary) into "
                    "a clean per-account profile — no manual login. Your daily "
                    "stable Chrome is never touched.")
    args = ap.parse_args()

    if args.login:
        login_flow(args.email)
        return

    session_copied = False
    if args.email:
        session_copied = use_account(args.email)

    if args.serve and not args.token:
        log("ERROR: --serve needs a worker token (--token or USER_WORKER_TOKEN). "
            "Get one from the platform: My Worker -> Advanced -> Worker Tokens.")
        return

    if not (args.quota or args.manual or args.jobs or args.prompt or args.serve):
        ap.print_help()
        return

    jobs = []
    if args.jobs:
        with open(args.jobs, encoding="utf-8") as f:
            jobs = json.load(f)
    elif args.prompt or args.manual:
        jobs = [{"prompt": args.prompt, "ref": args.ref, "out": args.out}]

    with _import_playwright()() as p:
        ctx, page = launch(p)
        try:
            if not ensure_logged_in(page, session_copied=session_copied):
                return
            _install(page)
            q = quota(page)
            if q:
                # Deliberately unlabelled — see quota() docstring. These are NOT
                # "N videos used of M"; that reading was tested and disproved.
                log(f"usage rpc (raw, meaning unverified): {q}")
            elif args.quota:
                log("usage rpc shape not recognised (qpEbW moved?)")
            if args.quota:
                return

            if args.serve:
                http_pull_loop(page, Api(args.api_url, args.token), once=args.once,
                               max_clips=args.max_clips, job_id=args.job_id)
                return

            ok, fails = 0, []
            for i, job in enumerate(jobs, 1):
                log(f"[{i}/{len(jobs)}] -> {job['out']}")
                try:
                    generate(page, job.get("prompt"), job.get("ref"), job["out"],
                             manual=args.manual)
                    ok += 1
                except Exception as e:
                    log(f"  x FAILED: {e}")
                    fails.append((i, str(e)))
                if i < len(jobs):
                    jitter(5, 10)
            log(f"DONE: {ok}/{len(jobs)} ok" + (f", failed={fails}" if fails else ""))
        finally:
            ctx.close()


if __name__ == "__main__":
    main()
