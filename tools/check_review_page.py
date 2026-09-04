"""v698A.3 browser proof — the review page shows b-roll in its own section.

Opens a real review page and asserts what the operator should see:
  * the A-roll grid holds exactly the spoken takes,
  * a B-roll section below holds every cutaway,
  * one group header per sentence the cutaways ride under,
  * ZERO paired cards (`.clip-paired`) anywhere,
  * every B-roll card says which words it covers.

The expected counts are derived from the job's own clips (read-only GET on
/api/jobs/<id>/clips), so this works on any job, not just the one it was
written for. --expect-* pins them by hand when you want to.

Read-only: it never clicks approve / redo / delete.

Auth: the page is behind Google OAuth, but main.py's AuthMiddleware also
accepts `Authorization: Bearer <worker token>`, so the browser context sends
that header on every request. The token is found the same way
send_to_platform.py finds it: KAVENO_API_TOKEN / VEO_TOKEN / USER_WORKER_TOKEN,
then ~/veo-worker/.env, then ~/.kaveno/token.

Browser: plain Playwright + headless Chromium. The workers reach for
patchright/camoufox (static/browser_driver.py) because Google fights
automation; our own site does not, so the plain driver is the right one here.

Usage (from code/):
    python tools/check_review_page.py --job d74ab616-ab21-4054-b121-a386fc2d823b \\
        --screenshot ../docs/audits/ui/2026-09-04-broll-section-d74ab616.png

    # before the deploy lands, prove the same code against the working copy:
    python tools/check_review_page.py --job <id> --local --screenshot <path>

Exit codes: 0 pass · 1 an assertion failed · 2 setup problem ·
3 production does not carry the change yet (deploy first, then re-run).
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BASE = "https://kavenobuilder.com"
TOKEN_KEYS = ("KAVENO_API_TOKEN", "VEO_TOKEN", "USER_WORKER_TOKEN")
MARKER = "function renderClipsPanel("      # the v698A.3 split


def resolve_token():
    for key in TOKEN_KEYS:
        val = os.environ.get(key, "").strip()
        if val:
            return val, f"env {key}"
    env_path = os.path.join(os.path.expanduser("~"), "veo-worker", ".env")
    try:
        with open(env_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                k, sep, v = line.strip().partition("=")
                if sep and k.strip() in TOKEN_KEYS:
                    v = v.strip().strip("\"'")
                    if v:
                        return v, "~/veo-worker/.env"
    except OSError:
        pass
    saved = os.path.join(os.path.expanduser("~"), ".kaveno", "token")
    try:
        with open(saved, encoding="utf-8") as f:
            val = f.read().strip()
        if val:
            return val, "~/.kaveno/token"
    except OSError:
        pass
    return None, None


def http_get(url, token, as_json=False):
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
    return json.loads(body.decode("utf-8")) if as_json else body.decode("utf-8", "replace")


def expectations(clips):
    """What the page should show for this job, straight from its clips."""
    has_lineup = any(c.get("in_lineup") is False for c in clips)
    visible = [c for c in clips if c.get("in_lineup") is not False] if has_lineup else list(clips)
    role = lambda c: (c.get("clip_role") or "").lower()
    broll = [c for c in visible if role(c) == "visual_pair"]
    aroll = [c for c in visible if role(c) != "visual_pair"]
    groups = {c.get("paired_clip_id") for c in broll}
    return {"aroll": len(aroll), "broll": len(broll), "groups": len(groups)}


# --------------------------------------------------------------------------
COUNT_JS = """() => {
    const panel = document.getElementById('clipsList');
    if (!panel) return {error: 'no #clipsList'};
    const grids = panel.querySelectorAll('.clips-grid');
    const broll = panel.querySelector('.broll-grid');
    const brollCards = broll ? Array.from(broll.querySelectorAll('.clip-card')) : [];
    return {
        grids: grids.length,
        aroll: grids.length ? grids[0].querySelectorAll('.clip-card').length : 0,
        broll: brollCards.length,
        groups: broll ? broll.querySelectorAll('.broll-group-header').length : 0,
        paired: panel.querySelectorAll('.clip-paired').length,
        covers: brollCards.filter(c => !!c.querySelector('.clip-text-covers')).length,
        title: (panel.querySelector('.broll-section-title') || {}).textContent || '',
        headers: broll ? Array.from(broll.querySelectorAll('.broll-group-header')).map(h => h.textContent.trim().slice(0, 70)) : [],
    };
}"""


def assert_counts(got, exp, failures):
    def check(label, actual, wanted):
        ok = actual == wanted
        print(f"  {'PASS' if ok else 'FAIL'}  {label}: {actual} (expected {wanted})")
        if not ok:
            failures.append(f"{label}: {actual} != {wanted}")

    check("A-roll cards", got["aroll"], exp["aroll"])
    check("B-roll cards", got["broll"], exp["broll"])
    check("B-roll group headers", got["groups"], exp["groups"])
    check("paired cards (.clip-paired)", got["paired"], 0)
    check("B-roll cards showing 'covers:'", got["covers"], exp["broll"])
    check("grids on the page", got["grids"], 2 if exp["broll"] else 1)


def shoot(target, path):
    if not path:
        return
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    target.screenshot(path=path)
    print(f"  screenshot -> {path}")


def run_production(args, token, clips):
    from playwright.sync_api import sync_playwright
    served = http_get(args.base + "/", token)
    if MARKER not in served:
        print("PENDING DEPLOY — the served page does not carry the v698A.3 split yet.")
        print(f"  looked for {MARKER!r} in {args.base}/ ({len(served)} bytes)")
        print("  ship the index.html commit, then re-run this exact command.")
        return 3
    print(f"served page carries the split ({len(served)} bytes)")
    exp = expectations(clips)
    url = f"{args.base}/?mode=review&job={args.job}"
    failures = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        ctx = browser.new_context(
            viewport={"width": 1600, "height": 1400},
            extra_http_headers={"Authorization": "Bearer " + token},
        )
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        print(f"opening {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_selector("#clipsList .clip-card", timeout=90000)
        if exp["broll"]:
            page.wait_for_selector("#clipsList .broll-grid .clip-card", timeout=90000)
        page.wait_for_timeout(2500)          # let the network render settle over the cache render
        got = page.evaluate(COUNT_JS)
        print(f"observed: {json.dumps(got, ensure_ascii=False)[:400]}")
        assert_counts(got, exp, failures)
        if errors:
            print(f"  NOTE page errors: {errors[:3]}")
        shoot(page, args.screenshot)
        browser.close()
    return 1 if failures else 0


def run_local(args, token, clips):
    """Same assertions against the working-copy index.html, real job data."""
    from playwright.sync_api import sync_playwright
    index = os.path.join(ROOT, "static", "index.html")
    src = open(index, encoding="utf-8").read()
    if MARKER not in src:
        print(f"SETUP: {index} does not contain {MARKER!r}")
        return 2
    exp = expectations(clips)
    failures = []
    stub = (
        "window.fetch = function(){ return Promise.resolve({ok:true,status:200,"
        "json:()=>Promise.resolve([]),text:()=>Promise.resolve('[]')}); };"
    )
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        ctx = browser.new_context(viewport={"width": 1600, "height": 1400})
        page = ctx.new_page()
        page.add_init_script(stub)
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.route("http*://**", lambda route: route.abort())
        page.goto("file:///" + index.replace("\\", "/"), timeout=90000)
        page.wait_for_function("typeof renderClipsPanel === 'function'", timeout=60000)
        page.evaluate(
            """([clips, jid]) => {
                cachedClipsData = clips;
                const p = renderClipsPanel(clips, jid, 'true');
                document.getElementById('clipsList').innerHTML =
                    '<div id="reviewBannerSlot"></div>' + p.html;
                const app = document.getElementById('appContainer');
                if (app) { app.className = 'app-container state-review'; }
            }""", [clips, args.job])
        got = page.evaluate(COUNT_JS)
        print(f"observed: {json.dumps(got, ensure_ascii=False)[:400]}")
        assert_counts(got, exp, failures)
        if errors:
            print(f"  NOTE page errors: {errors[:3]}")
        if args.screenshot:
            # #clipsList lives inside the app shell, which stays hidden until
            # the real mode router runs. Lift the panel onto a bare page so
            # the screenshot shows the section itself. (Videos stay blank —
            # a local run has no network.)
            page.evaluate("""() => {
                const panel = document.getElementById('clipsList');
                document.documentElement.style.cssText = 'overflow:auto;height:auto;';
                document.body.innerHTML = '';
                document.body.style.cssText = 'background:var(--bg-primary);overflow:auto;height:auto;';
                const wrap = document.createElement('div');
                wrap.style.cssText = 'padding:24px;';
                wrap.appendChild(panel);
                document.body.appendChild(wrap);
            }""")
            page.wait_for_timeout(300)
            path = os.path.abspath(args.screenshot)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            page.screenshot(path=path, full_page=True)
            print(f"  screenshot -> {path} (layout only; clip videos need the network)")
        browser.close()
    print("LOCAL RUN — this proves the code + this job's data, NOT the deployed page.")
    return 1 if failures else 0


def main():
    ap = argparse.ArgumentParser(description="v698A.3 review-page browser proof (read-only)")
    ap.add_argument("--job", required=True, help="job id to open")
    ap.add_argument("--base", default=DEFAULT_BASE, help=f"site root (default {DEFAULT_BASE})")
    ap.add_argument("--screenshot", default=None, help="where to save the screenshot")
    ap.add_argument("--local", action="store_true",
                    help="render the working-copy index.html with this job's clips instead of opening production")
    ap.add_argument("--headed", action="store_true", help="show the browser window (never minimise it)")
    ap.add_argument("--expect-aroll", type=int, default=None)
    ap.add_argument("--expect-broll", type=int, default=None)
    ap.add_argument("--expect-groups", type=int, default=None)
    args = ap.parse_args()

    token, where = resolve_token()
    if not token:
        print("SETUP: no worker token (env KAVENO_API_TOKEN / VEO_TOKEN / USER_WORKER_TOKEN, "
              "~/veo-worker/.env, or ~/.kaveno/token)")
        return 2
    print(f"token from {where}")
    try:
        clips = http_get(f"{args.base}/api/jobs/{args.job}/clips", token, as_json=True)
    except urllib.error.HTTPError as e:
        print(f"SETUP: GET /api/jobs/{args.job}/clips -> HTTP {e.code}")
        return 2
    except Exception as e:
        print(f"SETUP: could not read the job's clips ({type(e).__name__}: {e})")
        return 2
    print(f"job {args.job[:8]} has {len(clips)} clips")

    exp = expectations(clips)
    for key, val in (("aroll", args.expect_aroll), ("broll", args.expect_broll),
                     ("groups", args.expect_groups)):
        if val is not None:
            if val != exp[key]:
                print(f"  NOTE --expect-{key}={val} overrides the {exp[key]} derived from the job's clips")
            exp[key] = val
    print(f"expecting: {exp['aroll']} A-roll · {exp['broll']} B-roll · {exp['groups']} group headers · 0 paired")

    rc = run_local(args, token, clips) if args.local else run_production(args, token, clips)
    print("RESULT:", {0: "PASS", 1: "FAIL", 2: "SETUP ERROR", 3: "PENDING DEPLOY"}.get(rc, rc))
    return rc


if __name__ == "__main__":
    sys.exit(main())
