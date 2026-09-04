"""v698A.3 — the review page puts b-roll in its OWN section, no pairing cards.

Two halves, both run by `python tests/test_review_ui_broll_section.py` from code/:

STATIC (reads static/index.html as text)
  1. nothing assigns `_audioTwin` any more; renderClipPaired is defined but
     never called from loadClips / renderClip / renderClipsPanel;
  2. renderClipsPanel exists and BOTH loadClips render paths call it, and
     neither path builds a `.clips-grid` string of its own;
  3. renderBrollSection is emitted only under `brollClips.length`;
  4. renderClip derives b-roll from clip_role (no `opts` argument anywhere),
     shows `covers:` and drops the "paired" badge / "lip-syncs" tooltip;
  5. the poll's full-replacement call is still `targetCard.outerHTML =
     renderClip(clip, id)` — the same function, so a re-rendered cutaway can
     only come back as a b-roll card;
  6. every `.clips-grid` lookup is either querySelectorAll or one of the
     listed grid-agnostic ones; markClipRegenerating and toggleVariantsExpanded
     no longer do `querySelector('.clips-grid')`.

DOM (headless Chromium via Playwright, no network — index.html loaded from
disk with fetch stubbed)
  (a) 3 spoken + 3 cutaways -> two grids, three `covers:` cards, zero
      .clip-paired, one group header per sentence;
  (b) a status change replayed through the poll's `outerHTML = renderClip(...)`
      re-renders the cutaway as a b-roll card;
  (c) markClipRegenerating on a cutaway reaches the card in the SECOND grid;
  (d) a list with no visual_pair renders ONE grid whose markup is byte-identical
      to the same list rendered by the pre-v698A.3 index.html from git;
  (e) approveClipsSeq([a,b,c]) with the middle POST failing still posts a and c,
      names b in the failure banner, reloads once, and retries only b.
"""
import json
import os
import re
import subprocess
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, "static", "index.html")
SRC = open(INDEX, encoding="utf-8").read()


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def fn_body(name, src=None):
    """Text of a top-level (8-space indented) function in index.html."""
    src = SRC if src is None else src
    m = re.search(r"\n {8}(?:async )?function " + re.escape(name) + r"\s*\(", src)
    assert m, f"function {name} not found"
    start = m.start()
    nxt = re.search(r"\n {8}(?:async )?function \w+\s*\(", src[start + 1:])
    end = (start + 1 + nxt.start()) if nxt else len(src)
    body = src[start:end]
    assert len(body) > 40, f"function {name} body looks empty"
    return body


def run_git(args):
    r = subprocess.run(
        ["git"] + args, cwd=ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    return r.returncode, r.stdout


# --------------------------------------------------------------------------
# static checks
# --------------------------------------------------------------------------
def static_checks():
    # --- 1. no _audioTwin assignment left; renderClipPaired unreachable -----
    assigns = re.findall(r"\._audioTwin\s*=[^=]", SRC)
    assert not assigns, f"_audioTwin is still assigned {len(assigns)}x — the split must not attach a twin"
    assert "function renderClipPaired(" in SRC, "renderClipPaired stays DEFINED for one release (plan §3)"
    for caller in ("loadClips", "renderClip", "renderClipsPanel"):
        assert "renderClipPaired(" not in fn_body(caller), \
            f"{caller} still calls renderClipPaired — the paired card must be unreachable"
    # the paired helpers stay in the file too (removal is a follow-up)
    for kept in ("function approvePair(", "function redoPair("):
        assert kept in SRC, f"{kept} must stay defined (plan §3 out of scope)"
    print("OK 1: no _audioTwin attach anywhere; renderClipPaired/approvePair/redoPair defined but unreachable")

    # --- 2. one panel renderer, called by BOTH loadClips paths -------------
    assert "function renderClipsPanel(" in SRC
    load = fn_body("loadClips")
    calls = re.findall(r"renderClipsPanel\(", load)
    assert len(calls) == 2, f"loadClips must call renderClipsPanel twice (cached + network), found {len(calls)}"
    assert 'renderClipsPanel(cachedClips, id, ' in load, "the cached first-paint path must use the helper"
    assert 'renderClipsPanel(clips, id, ' in load, "the network path must use the helper"
    assert 'class="clips-grid' not in load, "loadClips must not build a .clips-grid string of its own any more"
    assert "_dedupePolicyViolations(" not in load, "the dedupe moved into renderClipsPanel"
    panel = fn_body("renderClipsPanel")
    assert "_dedupePolicyViolations(visible)" in panel, "the dedupe runs on the FULL visible list, before the split"
    assert 'class="${gridCls}" data-exports-loaded="${exportsLoaded}"' in panel, \
        "the A-roll grid keeps the caller's data-exports-loaded value (v701l)"
    print("OK 2: renderClipsPanel is the single renderer; both loadClips paths call it")

    # --- 3. the B-roll section is conditional on brollClips.length ---------
    assert "(brollClips.length ? renderBrollSection(" in panel, \
        "the B-roll section may only render when there are cutaways"
    assert "function renderBrollSection(" in SRC
    broll = fn_body("renderBrollSection")
    assert "🎬 B-roll (${brollClips.length})" in broll, "the section heading names how many cutaways there are"
    assert "broll-group-header" in broll and "under #${" in broll, "each run of cutaways gets its sentence subheader"
    assert "under — (no spoken clip)" in broll, "an orphan cutaway gets its own group"
    assert "approveClipsSeq(" in broll, "per-group and approve-all buttons run the sequential approve"
    assert 'class="${gridCls} broll-grid"' in broll, "the second grid shares .clips-grid so expand/collapse applies"
    print("OK 3: renderBrollSection is conditional, grouped by sentence, and shares the grid class")

    # --- 4. renderClip derives b-roll from the clip itself ------------------
    rc = fn_body("renderClip")
    assert "const isBroll = (c.clip_role || '').toLowerCase() === 'visual_pair';" in rc, \
        "b-roll mode must be derived from clip_role, not passed in"
    assert not re.search(r"function renderClip\s*\(\s*c\s*,\s*jid\s*,", SRC), \
        "renderClip keeps its two-argument signature"
    assert "opts.broll" not in SRC and "opts.isBroll" not in SRC, "no opts argument anywhere"
    assert 'covers: \\"${escapeHtml(' in rc or 'covers: "${escapeHtml(' in rc, \
        "a cutaway card says which WORDS it covers"
    assert "🎙️ paired" not in rc, "the paired badge is gone from the standalone card"
    assert "lip-syncs this line" not in rc, "the audio-twin tooltip is gone from the standalone card"
    assert 'data-clip-id="${clipId}" data-clip-index="${c.clip_index}"' in rc, \
        "the card keeps data-clip-id + data-clip-index so every existing handler still finds it"
    print("OK 4: renderClip derives isBroll, shows covers:, drops the paired badge, keeps its data attributes")

    # --- 5. the poll re-renders through the SAME function -------------------
    assert load.count("targetCard.outerHTML = renderClip(clip, id);") == 1, \
        "the poll's full replacement must still call renderClip(clip, id)"
    assert load.count('c.querySelector(`.clip-card[data-clip-index="${clip.clip_index}"]`)') == 2, \
        "both poll lookups must search the whole panel, not the first grid"
    assert 'container.querySelector(`.clip-card' not in load, \
        "no card lookup may be scoped to the A-roll grid"
    print("OK 5: the poll patches cards panel-wide and re-renders them through renderClip")

    # --- 6. every .clips-grid lookup is grid-agnostic or converted ----------
    GRID_AGNOSTIC = {
        # (function, why it is fine with two grids)
        "loadClips": "the '#clipsList .clips-grid' guard only asks whether ANY grid is painted; "
                     "`c.querySelector('.clips-grid')` is the A-roll grid used as the toolbar anchor "
                     "and the exports-loaded flag, never for card lookups",
        "playWithAudio": "presence check for '.clips-grid.expanded-mode' — both grids carry the class",
        "stopWithAudio": "same presence check",
    }
    CONVERTED = {"markClipRegenerating", "toggleVariantsExpanded"}
    script = SRC[SRC.index("<script>", SRC.index("</style>")):]
    hits = [m for m in re.finditer(r"querySelector(All)?\((['\"`])([^'\"`]*\.clips-grid[^'\"`]*)\2\)", script)]
    assert hits, "expected .clips-grid lookups to exist"
    for m in hits:
        fn = None
        for name in list(GRID_AGNOSTIC) + sorted(CONVERTED):
            if m.group(0) in fn_body(name):
                fn = name
                break
        assert fn is not None, f"unattributed .clips-grid lookup: {m.group(0)}"
        if fn in CONVERTED:
            assert m.group(1) == "All", f"{fn} must use querySelectorAll, found {m.group(0)}"
    for name in CONVERTED:
        body = fn_body(name)
        assert "querySelector('.clips-grid')" not in body and 'querySelector(".clips-grid")' not in body, \
            f"{name} still looks up only the FIRST grid"
    assert "document.getElementById('clipsList')" in fn_body("markClipRegenerating"), \
        "markClipRegenerating must search the whole panel"
    assert "document.querySelectorAll('.clips-grid').forEach" in fn_body("toggleVariantsExpanded"), \
        "expand/collapse must toggle BOTH grids"
    print(f"OK 6: all {len(hits)} .clips-grid lookups accounted for; markClipRegenerating + toggleVariantsExpanded converted")

    # --- 7. approveClipsSeq: one bad id must not stop the batch -------------
    seq = fn_body("approveClipsSeq")
    assert "_approveSeqRunning" in seq, "re-entrancy guard"
    assert "} finally {" in seq and "await loadClips(jid)" in seq, "loadClips runs in finally"
    assert "failed.push(cid)" in seq, "a failure is recorded, not thrown"
    assert "for (const cid of list)" in seq, "the batch is sequential"
    assert "_renderBrollApproveError(failed, firstError)" in seq, "failures surface in one banner"
    err = fn_body("_renderBrollApproveError")
    assert "Could not approve" in err and "approveClipsSeq([${failedIds.join(',')}])" in err, \
        "the banner names the failures and retries only those"
    print("OK 7: approveClipsSeq is sequential, records failures, reloads in finally, retries only the failures")


# --------------------------------------------------------------------------
# DOM checks (headless Chromium, no network)
# --------------------------------------------------------------------------
INIT_SCRIPT = """
window.__fetchLog = [];
window.__fetchFail = null;      // url substring that should fail
window.fetch = function (url, opts) {
  const u = String(url);
  window.__fetchLog.push({url: u, method: (opts && opts.method) || 'GET'});
  if (window.__fetchFail && u.indexOf(window.__fetchFail) !== -1) {
    return Promise.resolve({
      ok: false, status: 500,
      json: () => Promise.resolve({detail: 'clip is still rendering'}),
      text: () => Promise.resolve('{}')
    });
  }
  return Promise.resolve({
    ok: true, status: 200,
    json: () => Promise.resolve([]), text: () => Promise.resolve('[]')
  });
};
"""

SPOKEN = [
    {"id": 101, "clip_index": 0, "clip_role": None, "status": "completed",
     "approval_status": "pending_review", "dialogue_text": "hi, i'm nora and i made these acrylic painting guides.",
     "output_filename": "c0.mp4", "attempts_remaining": 2, "total_variants": 1, "versions": []},
    {"id": 102, "clip_index": 1, "clip_role": None, "status": "completed",
     "approval_status": "pending_review",
     "dialogue_text": "i always wanted a creative outlet that was just mine and nobody else's.",
     "output_filename": "c1.mp4", "attempts_remaining": 2, "total_variants": 1, "versions": []},
    {"id": 103, "clip_index": 2, "clip_role": None, "status": "completed",
     "approval_status": "pending_review", "dialogue_text": "painting felt expressive, freeing, and honest.",
     "output_filename": "c2.mp4", "attempts_remaining": 2, "total_variants": 1, "versions": []},
]
CUTAWAYS = [
    {"id": 201, "clip_index": 3, "clip_role": "visual_pair", "paired_clip_id": 102,
     "status": "completed", "approval_status": "pending_review", "dialogue_text": "",
     "voiceover_line": "i always wanted a creative outlet", "output_filename": "b0.mp4",
     "attempts_remaining": 2, "total_variants": 1, "versions": []},
    {"id": 202, "clip_index": 4, "clip_role": "visual_pair", "paired_clip_id": 102,
     "status": "completed", "approval_status": "pending_review", "dialogue_text": "",
     "voiceover_line": "that was just mine", "output_filename": "b1.mp4",
     "attempts_remaining": 2, "total_variants": 1, "versions": []},
    {"id": 203, "clip_index": 5, "clip_role": "visual_pair", "paired_clip_id": 103,
     "status": "completed", "approval_status": "pending_review", "dialogue_text": "",
     "voiceover_line": "painting felt expressive,", "output_filename": "b2.mp4",
     "attempts_remaining": 2, "total_variants": 1, "versions": []},
]
ALL_CLIPS = SPOKEN + CUTAWAYS


def _open(pw, path):
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()
    page.add_init_script(INIT_SCRIPT)
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.route("http*://**", lambda route: route.abort())   # fonts etc; file:// untouched
    page.goto("file:///" + path.replace("\\", "/"))
    page.wait_for_function("typeof renderClipsPanel === 'function'")
    return browser, page, errors


def dom_checks():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:  # pragma: no cover
        print("SKIP DOM: playwright not installed")
        return

    with sync_playwright() as pw:
        browser, page, errors = _open(pw, INDEX)
        try:
            # --- (a) two grids, covers: cards, no paired card --------------
            res = page.evaluate(
                """(clips) => {
                    const p = renderClipsPanel(clips, 'job-x', 'true');
                    document.getElementById('clipsList').innerHTML = p.html;
                    const grids = document.querySelectorAll('#clipsList .clips-grid');
                    const broll = document.querySelector('#clipsList .broll-grid');
                    return {
                        grids: grids.length,
                        aroll: grids[0].querySelectorAll('.clip-card').length,
                        brollCards: broll.querySelectorAll('.clip-card').length,
                        headers: broll.querySelectorAll('.broll-group-header').length,
                        headerText: Array.from(broll.querySelectorAll('.broll-group-header')).map(h => h.textContent.trim()),
                        covers: document.querySelectorAll('#clipsList .clip-text-covers').length,
                        paired: document.querySelectorAll('#clipsList .clip-paired').length,
                        title: document.querySelector('.broll-section-title').textContent,
                        arollIds: Array.from(grids[0].querySelectorAll('.clip-card')).map(c => c.dataset.clipId),
                        brollIds: Array.from(broll.querySelectorAll('.clip-card')).map(c => c.dataset.clipId),
                    };
                }""", ALL_CLIPS)
            assert res["grids"] == 2, res
            assert res["aroll"] == 3 and res["brollCards"] == 3, res
            assert res["headers"] == 2, res
            assert res["headerText"][0].startswith("under #2"), res["headerText"]
            assert res["headerText"][1].startswith("under #3"), res["headerText"]
            assert res["covers"] == 3, res
            assert res["paired"] == 0, res
            assert res["title"] == "🎬 B-roll (3)", res
            assert res["arollIds"] == ["101", "102", "103"], res
            assert res["brollIds"] == ["201", "202", "203"], res
            print("OK a: 3 spoken in grid 1, 3 cutaways in the B-roll grid under 2 sentence headers, 0 paired cards")

            # --- (b) poll replacement keeps it a b-roll card ---------------
            res_b = page.evaluate(
                """(clips) => {
                    const changed = Object.assign({}, clips[3], {status: 'completed', approval_status: 'approved'});
                    const card = document.querySelector('#clipsList .clip-card[data-clip-index="3"]');
                    const inBrollBefore = !!card.closest('.broll-grid');
                    card.outerHTML = renderClip(changed, 'job-x');
                    const after = document.querySelector('#clipsList .clip-card[data-clip-index="3"]');
                    return {
                        inBrollBefore,
                        inBrollAfter: !!after.closest('.broll-grid'),
                        covers: !!after.querySelector('.clip-text-covers'),
                        text: after.querySelector('.clip-text').textContent.trim(),
                        paired: after.classList.contains('clip-paired'),
                        badge: after.querySelector('.badge').textContent.trim(),
                        clipId: after.dataset.clipId,
                    };
                }""", ALL_CLIPS)
            assert res_b["inBrollBefore"] and res_b["inBrollAfter"], res_b
            assert res_b["covers"] and res_b["text"].startswith('covers: "i always wanted'), res_b
            assert res_b["paired"] is False and res_b["badge"] == "approved", res_b
            assert res_b["clipId"] == "201", res_b
            print("OK b: the poll's outerHTML replacement re-renders the cutaway as a b-roll card, in place")

            # --- (c) markClipRegenerating reaches the second grid ----------
            res_c = page.evaluate(
                """() => {
                    markClipRegenerating(4);
                    const card = document.querySelector('#clipsList .clip-card[data-clip-index="4"]');
                    return {
                        inBroll: !!card.closest('.broll-grid'),
                        queued: card.classList.contains('clip-redo_queued'),
                        badge: card.querySelector('.badge').textContent.trim(),
                        spinner: !!card.querySelector('.generating-spinner'),
                    };
                }""")
            assert res_c == {"inBroll": True, "queued": True, "badge": "redo queued", "spinner": True}, res_c
            print("OK c: markClipRegenerating paints the queued state on a card in the B-roll grid")

            # --- (d) no visual_pair -> ONE grid, byte-identical markup -----
            code, intro = run_git(["log", "--reverse", "--format=%H", "-S",
                                   "function renderClipsPanel(", "--", "static/index.html"])
            base_rev = (intro.split() or ["HEAD"])[0] + "^" if intro.strip() else "HEAD"
            code, base_src = run_git(["show", f"{base_rev}:static/index.html"])
            assert code == 0 and base_src, f"could not read baseline index.html at {base_rev}"
            assert "function renderClipsPanel(" not in base_src, \
                f"{base_rev} already has the split — not a pre-v698A.3 baseline"
            base_path = os.path.join(ROOT, "tests", "_baseline_index_v698a3.html")
            with open(base_path, "w", encoding="utf-8", newline="") as f:
                f.write(base_src)
            try:
                b2, page2, err2 = _open_baseline(pw, base_path)
                try:
                    old_html = page2.evaluate(
                        """(clips) => {
                            // exactly what pre-v698A.3 loadClips built for a
                            // list with no visual_pair (no twin attach, no
                            // audio_pair, dedupe is a no-op here)
                            const visible = _dedupePolicyViolations(clips);
                            return `<div class="clips-grid ${variantsExpanded ? 'expanded-mode' : ''}" data-exports-loaded="true">`
                                 + visible.map(x => renderClip(x, 'job-x')).join('') + `</div>`;
                        }""", SPOKEN)
                finally:
                    b2.close()
                new = page.evaluate(
                    """(clips) => {
                        const p = renderClipsPanel(clips, 'job-x', 'true');
                        const d = document.createElement('div');
                        d.innerHTML = p.html;
                        return {html: p.html, grids: d.querySelectorAll('.clips-grid').length,
                                broll: d.querySelectorAll('.broll-section').length,
                                bcount: p.brollClips.length};
                    }""", SPOKEN)
                assert new["grids"] == 1 and new["broll"] == 0 and new["bcount"] == 0, new
                assert new["html"] == old_html, (
                    "markup drifted for a job with no b-roll\n"
                    f"  new {len(new['html'])} bytes vs baseline {len(old_html)} bytes")
                code, base_sha = run_git(["rev-parse", "--short", base_rev])
                shown = base_sha.strip() or base_rev
                print(f"OK d: a job with no visual_pair renders ONE grid, byte-identical to {shown} ({len(old_html)} bytes)")
            finally:
                if os.path.exists(base_path):
                    os.remove(base_path)

            # --- (e) approveClipsSeq: one failure does not stop the batch --
            res_e = page.evaluate(
                """(clips) => new Promise(async (resolve) => {
                    cachedClipsData = clips;
                    selectedJobId = 'job-x';
                    window.__loadClipsCalls = 0;
                    window.loadClips = function (id) { window.__loadClipsCalls++; return Promise.resolve(id); };
                    const p = renderClipsPanel(clips, 'job-x', 'true');
                    document.getElementById('clipsList').innerHTML = '<div id="reviewBannerSlot"></div>' + p.html;
                    window.__fetchLog = [];
                    window.__fetchFail = '/clips/202/approve';
                    await approveClipsSeq([201, 202, 203]);
                    const posts = window.__fetchLog.filter(f => f.method === 'POST').map(f => f.url);
                    const banner = document.getElementById('brollApproveError');
                    const firstRun = {
                        posts,
                        loads: window.__loadClipsCalls,
                        banner: banner ? banner.textContent.trim() : null,
                    };
                    // the retry button must re-post ONLY the failure
                    window.__fetchLog = [];
                    window.__fetchFail = null;
                    banner.querySelector('button').click();
                    setTimeout(() => {
                        resolve({
                            firstRun,
                            retryPosts: window.__fetchLog.filter(f => f.method === 'POST').map(f => f.url),
                            loadsAfterRetry: window.__loadClipsCalls,
                            bannerGone: !document.getElementById('brollApproveError'),
                        });
                    }, 300);
                })""", ALL_CLIPS)
            posts = res_e["firstRun"]["posts"]
            assert len(posts) == 3, posts
            assert posts[0].endswith("/clips/201/approve") and posts[2].endswith("/clips/203/approve"), posts
            assert res_e["firstRun"]["loads"] == 1, res_e["firstRun"]
            banner = res_e["firstRun"]["banner"]
            assert banner and "Could not approve #5" in banner, banner
            assert "clip is still rendering" in banner, banner
            assert res_e["retryPosts"] == [p for p in res_e["retryPosts"] if p.endswith("/clips/202/approve")], res_e
            assert len(res_e["retryPosts"]) == 1, res_e
            assert res_e["loadsAfterRetry"] == 2 and res_e["bannerGone"], res_e
            print("OK e: a failing approve is recorded, the batch continues, the banner names it, retry re-posts only it")

            assert not errors, f"page errors: {errors[:3]}"
        finally:
            browser.close()


def _open_baseline(pw, path):
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()
    page.add_init_script(INIT_SCRIPT)
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.route("http*://**", lambda route: route.abort())
    page.goto("file:///" + path.replace("\\", "/"))
    page.wait_for_function("typeof renderClip === 'function'")
    return browser, page, errors


def test_review_ui_broll_section():
    """pytest entry point — same body as the script run."""
    static_checks()
    dom_checks()


if __name__ == "__main__":
    static_checks()
    dom_checks()
    print("ALL OK — v698A.3 b-roll section")
