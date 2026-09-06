"""v962 — Flow moved to flow.google.com; both workers must recognise BOTH hosts.

Why a test: on 2026-09-05 every URL predicate in both workers sat on
labs.google/fx/tools/flow. On the new host a signed-in project page was
classified 'other', the worker looped "Not on Flow — navigating...", then the
ULTRA badge probe (written for the old nav) returned a false not_ultra and
killed the account. 82 clips owed, none submitted, on an Ultra account.

These tests exec the predicates out of each worker's SOURCE, because the
workers are standalone scripts that cannot be imported without a browser.
"""
import ast
import os
import re
import textwrap

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_predicates(path):
    """Pull FLOW_ORIGIN / FLOW_HOME_URL and the four is_flow_* defs out of a
    worker source file and exec just those, so no browser is needed."""
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src)
    wanted = {"is_flow_url", "is_flow_home", "is_flow_project",
              "is_google_login", "is_on_flow_not_login"}
    pieces = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            pieces.append(ast.get_source_segment(src, node))
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in (
                        "FLOW_ORIGIN", "FLOW_HOME_URL", "FLOW_HOME_URL_LEGACY"):
                    pieces.append(ast.get_source_segment(src, node))
    ns = {}
    exec(textwrap.dedent("\n".join(pieces)), ns)
    for name in wanted | {"FLOW_ORIGIN", "FLOW_HOME_URL"}:
        assert name in ns, f"{os.path.basename(path)}: {name} not found"
    return ns


WORKERS = [
    os.path.join(HERE, "static", "flow_worker.py"),
    os.path.join(HERE, "image_worker.py"),
]

NEW_HOME = "https://flow.google.com/"
NEW_PROJECT = "https://flow.google.com/project/26a09d34-7066-41e2-bf5a-69f5eb0da84e"
OLD_HOME = "https://labs.google/fx/tools/flow"
OLD_HOME_LOCALE = "https://labs.google/fx/es-419/tools/flow"
OLD_PROJECT = "https://labs.google/fx/en/tools/flow/project/abc-123"
LOGIN = "https://accounts.google.com/v3/signin/identifier?continue=https://flow.google.com/"
ELSEWHERE = "https://www.google.com/"


def test_v962_both_workers_recognise_the_new_host():
    for path in WORKERS:
        ns = _load_predicates(path)
        tag = os.path.basename(path)
        assert ns["is_flow_url"](NEW_HOME), tag
        assert ns["is_flow_url"](NEW_PROJECT), tag
        assert ns["is_flow_home"](NEW_HOME), tag
        assert not ns["is_flow_home"](NEW_PROJECT), tag
        assert ns["is_flow_project"](NEW_PROJECT), tag
        assert not ns["is_flow_project"](NEW_HOME), tag
        assert ns["is_on_flow_not_login"](NEW_PROJECT), tag


def test_v962_legacy_host_still_legal():
    """Rollout was partial on 09-05 and redirect chains pass through the old
    host, so it must keep working."""
    for path in WORKERS:
        ns = _load_predicates(path)
        tag = os.path.basename(path)
        for u in (OLD_HOME, OLD_HOME_LOCALE, OLD_PROJECT):
            assert ns["is_flow_url"](u), (tag, u)
        assert ns["is_flow_project"](OLD_PROJECT), tag
        assert ns["is_flow_home"](OLD_HOME), tag


def test_v962_login_and_elsewhere_are_not_flow():
    for path in WORKERS:
        ns = _load_predicates(path)
        tag = os.path.basename(path)
        # a Google login page whose `continue=` carries the Flow host must
        # still read as LOGIN, not as Flow
        assert ns["is_google_login"](LOGIN), tag
        assert not ns["is_on_flow_not_login"](LOGIN), tag
        assert not ns["is_flow_url"](ELSEWHERE), tag


def test_v962_home_and_origin_point_at_the_new_host():
    for path in WORKERS:
        ns = _load_predicates(path)
        tag = os.path.basename(path)
        assert ns["FLOW_ORIGIN"] == "https://flow.google.com", tag
        assert ns["FLOW_HOME_URL"].startswith("https://flow.google.com"), tag


def test_v962_no_project_url_is_still_built_on_the_old_host():
    """The builders navigate the browser; a stale one would goto labs.google
    and rely on a redirect that the partial rollout does not guarantee."""
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        stale = re.findall(r'f"https://labs\.google/fx/tools/flow/project/\{', src)
        assert not stale, f"{os.path.basename(path)}: {len(stale)} project URL(s) still built on labs.google"


def test_v962_video_worker_does_not_kill_on_missing_badge_on_new_host():
    src = open(WORKERS[0], encoding="utf-8").read()
    i = src.index("def check_ultra_account(")
    # the function is long; take it up to the next top-level def
    j = src.find("\ndef ", i + 10)
    body = src[i:j if j != -1 else len(src)]
    assert '"flow.google.com" in _cur_url' in body, \
        "check_ultra_account has no new-host carve-out — a missing badge would still kill an Ultra account"
    # the REAL kill is the print + worker-error POST, not the comment that
    # quotes an old log line higher up in the same function
    kill = body.index("cannot use Flow")
    carve = body.index('"flow.google.com" in _cur_url')
    assert carve < kill, "the new-host carve-out must come BEFORE the kill"
    # v962.1 — and an EARLY return must precede the poll loop itself. On the
    # pre-v962 build the process hung inside the two poll rounds for ~14 min
    # (last line "ULTRA badge not seen yet — reloading + re-polling..."), so a
    # branch placed after them is reachable only by luck.
    early = body.index('"flow.google.com" in _early_url')
    poll = body.index("for _round in range(2)")
    assert early < poll, "the new-host decision must come BEFORE the badge poll, not after it"


def test_v962_2_spa_navigation_never_pushes_the_old_home_path():
    """v962.2 — the SPA home navigation must not push '/fx/tools/flow' as a
    literal. On flow.google.com that route is the app's client-side 404 page,
    which has no "New project" button, so the DOM click times out and the worker
    reads flow.google.com/404 as the project URL (martha 8b800f8b, 2026-09-06)."""
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        i = src.index("def spa_navigate_to_flow_home(")
        j = src.find("\ndef ", i + 10)
        body = src[i:j if j != -1 else len(src)]
        assert "router.push('/fx/tools/flow')" not in body, os.path.basename(path)
        assert "pushState({}, '', '/fx/tools/flow')" not in body, os.path.basename(path)
        # only a home nav that PUSHES a path must derive it from the host; the
        # image worker's home nav clicks anchors and gotos FLOW_HOME_URL only
        if "pushState(" in body or "router.push(" in body:
            assert "flow_home_path(" in body, \
                f"{os.path.basename(path)}: spa_navigate_to_flow_home pushes a path but does not derive it from the host"
        # 27's addition: pin the literal's absence anywhere in the function
        assert "/fx/tools/flow" not in body.replace("a[href*='/tools/flow']", "").replace("a[href='/fx/tools/flow']", ""), \
            f"{os.path.basename(path)}: a '/fx/tools/flow' path literal survives outside the legacy anchor selectors"


def test_v962_2_project_path_is_host_aware():
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        i = src.index("def _fa_spa_navigate_to_project(")
        j = src.find("\ndef ", i + 10)
        body = src[i:j if j != -1 else len(src)]
        assert 'f"/fx/tools/flow/project/{pid}"' not in body, \
            f"{os.path.basename(path)}: project SPA path is still the legacy literal"
        assert "flow_project_path(" in body, os.path.basename(path)


def test_v962_2_path_helpers_answer_per_host():
    ns_all = []
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tree = ast.parse(src)
        pieces = [ast.get_source_segment(src, n) for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name in ("flow_home_path", "flow_project_path")]
        assert len(pieces) == 2, os.path.basename(path)
        ns = {}
        exec("\n".join(pieces), ns)
        assert ns["flow_home_path"](NEW_PROJECT) == "/"
        assert ns["flow_home_path"](OLD_PROJECT) == "/fx/tools/flow"
        assert ns["flow_project_path"](NEW_HOME, "abc") == "/project/abc"
        assert ns["flow_project_path"](OLD_HOME, "abc") == "/fx/tools/flow/project/abc"


# ---------------------------------------------------------------------------
# v962.3 — the settings overlay on flow.google.com is Angular Material.
# Measured selectors (session 9e4b16cc, 2026-09-06): chip
# button[aria-label='Settings trigger'], overlay .cdk-overlay-container with
# [role='radiogroup'] / button[role='radio'] (aria-checked), model menu behind
# button[aria-label='Select model family'] with [role='menuitem'] items.
# ---------------------------------------------------------------------------

def _func_body(src, name):
    i = src.index(f"def {name}(")
    j = src.find("\ndef ", i + 10)
    return src[i:j if j != -1 else len(src)]


def test_v962_3_both_workers_carry_the_material_settings_resolver():
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tag = os.path.basename(path)
        for fn in ("_v962_on_new_host", "_v962_open_settings", "_v962_pick_radio", "_v962_pick_model"):
            assert f"def {fn}(" in src, (tag, fn)
        assert "aria-label='Settings trigger'" in src, tag
        assert "button[role='radio']" in src, tag
        assert '"aria-checked"' in src, tag
        assert "aria-label='Select model family'" in src, tag
        assert ".cdk-overlay-container" in src, tag
        # the menu-item selector lives in the module constant the picker reads
        assert "_V962_MENU_ITEMS = " in src and "[role='menuitem']" in src, tag
        assert "_V962_MENU_ITEMS" in _func_body(src, "_v962_pick_model"), tag


def test_v962_3_video_settings_branch_precedes_the_legacy_retry_loop():
    src = open(WORKERS[0], encoding="utf-8").read()
    body = _func_body(src, "select_frames_to_video_mode")
    branch = body.index("_v962_material_video_settings(")
    loop = body.index("for full_attempt in range(3)")
    assert branch < loop, "the new-host resolver must run BEFORE the Radix retry loop"
    assert "_v962_on_new_host(page)" in body
    # the resolver keeps the same criticality as the legacy pass (v945.15)
    res = _func_body(src, "_v962_material_video_settings")
    # v962.7 — the input mode is critical again (Video type radio), Model for Omni
    assert "critical = ['Video', mode_key, 'Portrait']" in res
    assert 'if target_model == "Omni Flash":' in res
    # Ingredients is a DELIBERATE hold on the new host, said in the log line and
    # stashed where v945.15 records it (the sentence lives in a module constant)
    assert "UNMEASURED on" in src and "deliberate hold" in src
    assert "_V962_INGREDIENTS_HOLD" in res
    # and the legacy path is still there, untouched in shape
    assert "button.flow_tab_slider_trigger" in body
    assert "button:has-text('x{n}')" in body


def test_v962_3_image_settings_functions_branch_on_the_new_host():
    src = open(WORKERS[1], encoding="utf-8").read()
    for fn in ("_open_settings_dropdown", "select_image_mode", "configure_image_settings"):
        body = _func_body(src, fn)
        assert "_v962_on_new_host(page)" in body, fn
    assert "_v962_material_image_settings(" in _func_body(src, "configure_image_settings")
    # legacy Radix path still present
    assert '"data-state"' in _func_body(src, "_open_settings_dropdown")
    assert "flow_tab_slider_trigger" in _func_body(src, "select_image_mode")


def test_v962_4_only_the_root_path_is_home_on_the_new_host():
    """v962.4 — flow.google.com/about (the marketing page) was read as home and
    the worker waited there for a New-project button that page never has."""
    for path in WORKERS:
        ns = _load_predicates(path)
        tag = os.path.basename(path)
        assert ns["is_flow_home"](NEW_HOME), tag
        assert ns["is_flow_home"]("https://flow.google.com"), tag
        assert ns["is_flow_home"]("https://flow.google.com/?hl=en"), tag
        assert not ns["is_flow_home"]("https://flow.google.com/about"), tag
        assert not ns["is_flow_home"]("https://flow.google.com/404"), tag
        assert not ns["is_flow_home"](NEW_PROJECT), tag
        # legacy host unchanged
        assert ns["is_flow_home"](OLD_HOME), tag
        assert ns["is_flow_home"](OLD_HOME_LOCALE), tag


def test_v962_4_login_proof_on_the_new_host_is_the_dom_not_the_cookie():
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tag = os.path.basename(path)
        assert "def _v962_enter_app(" in src, tag
        assert "button[aria-label^='Google Account:']" in _func_body(src, "_get_page_state") \
            or "button[aria-label^='Google Account:']" in src[src.index("logged_in_selectors = ["):src.index("logged_in_selectors = [") + 400], tag
        # EVERY not-logged-in branch reaches the passive handoff BEFORE its CTA
        # list — the video worker has two (ensure_logged_into_flow and the
        # user-login wait inside it)
        sites = [m.start() for m in re.finditer(r"entry_selectors = \[", src)]
        assert sites, tag
        for i in sites:
            window = src[max(0, i - 900):i]
            assert "_v962_enter_app(" in window, \
                f"{tag}: a not-logged-in branch at offset {i} does not go through the passive handoff first"
        # and the handoff helper never clicks the CTA
        body = _func_body(src, "_v962_enter_app")
        assert "Create with" not in body, tag
        assert "ServiceLogin" in body, tag


def test_v962_5_model_menu_measured_on_the_new_host_resolves():
    """v962.5 — measured 2026-09-06 (worker_fg_0906c.log): the menu lists
    'Omni 1.1 Flash' / 'Veo 3.1 - Lite' / 'Veo 3.1 - Fast' / 'Veo 3.1 - Quality',
    each with a Material icon label on its own first line, and no
    '[Lower Priority]' entry. Every platform job value must resolve."""
    menu = ["volume_up\nOmni 1.1 Flash", "volume_up\nVeo 3.1 - Lite",
            "volume_up\nVeo 3.1 - Fast", "volume_up\nVeo 3.1 - Quality"]
    expect = {"Omni Flash": "omni 1.1 flash",
              "Veo 3.1 - Lite [Lower Priority]": "veo 3.1 - lite",
              "Veo 3.1 - Lite": "veo 3.1 - lite",
              "Veo 3.1 - Fast": "veo 3.1 - fast",
              "Veo 3.1 - Quality": "veo 3.1 - quality"}
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tree = ast.parse(src)
        pieces = [ast.get_source_segment(src, n) for n in tree.body
                  if (isinstance(n, ast.FunctionDef) and n.name in ("_v962_norm_model", "_v962_item_label"))
                  or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "_V962_MODEL_ALIASES" for t in n.targets))]
        assert len(pieces) == 3, os.path.basename(path)
        ns = {"re": re}
        exec("\n".join(pieces), ns)
        labels = [ns["_v962_norm_model"](ns["_v962_item_label"](m)) for m in menu]
        assert labels == ["omni 1.1 flash", "veo 3.1 - lite", "veo 3.1 - fast", "veo 3.1 - quality"], os.path.basename(path)
        for target, want in expect.items():
            t = ns["_v962_norm_model"](target)
            wanted = ns["_V962_MODEL_ALIASES"].get(t, (t,))
            pick = next((l for l in labels if l in wanted), None) or \
                next((l for l in labels if any(w in l for w in wanted)), None)
            assert pick == want, (os.path.basename(path), target, pick)
        # and the picker itself uses the alias table + label stripper
        body = _func_body(src, "_v962_pick_model")
        assert "_V962_MODEL_ALIASES" in body and "_v962_item_label(" in body, os.path.basename(path)


def test_v962_6_project_id_is_read_from_the_dom_when_the_url_does_not_move():
    """v962.6 — on flow.google.com the New-project click renders the project in
    place (Start generation / settings chip / Tools link /project/<uuid>/tools)
    while page.url stays '/'. Both workers must carry the DOM reader, gated on a
    generation control so a home-list tile is never mistaken for a new project."""
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tag = os.path.basename(path)
        body = _func_body(src, "_v962_project_id_from_dom")
        assert "Start generation" in body and "Settings trigger" in body, tag
        assert "/tools" in body, tag
    fw = open(WORKERS[0], encoding="utf-8").read()
    click = _func_body(fw, "_fa_or_dom_new_project_click")
    assert "_v962_project_id_from_dom(page)" in click
    assert 'f"{FLOW_ORIGIN}/project/{_pid}"' in click
    iw = open(WORKERS[1], encoding="utf-8").read()
    assert iw.count("_v962_project_id_from_dom(page)") >= 1
    assert 'f"{FLOW_ORIGIN}/project/{_pid}"' in iw


def test_v962_7_frames_prompt_and_generate_branch_on_the_new_host():
    """v962.7 — measured 2026-09-06 on the worker's own profile: the overlay's
    'Video type' radios (Frames / Ingredients), the composer's Start/End frame
    slots + picker (asset options, 'Upload media' file chooser), the rich-text
    prompt editor and button[aria-label='Start generation']. Every legacy site
    that walked the dead aria-haspopup="dialog" path must branch on the host."""
    src = open(WORKERS[0], encoding="utf-8").read()
    for fn in ("_v962_attach_frame", "_v962_pick_asset_in_picker", "_v962_type_prompt", "_v962_generate_enabled"):
        assert f"def {fn}(" in src, fn
    assert "flow-ingredient-bar button:has-text('Start')" in src
    assert "flow-rich-text-editor [contenteditable='true']" in src
    assert "button[aria-label='Start generation']" in src
    assert "expect_file_chooser(" in _func_body(src, "_v962_pick_asset_in_picker")
    # the resolver picks the Video type radio and keeps the mode critical again
    res = _func_body(src, "_v962_material_video_settings")
    assert '_v962_pick_radio(page, "Frames", "Video type", prefix)' in res
    assert "critical = ['Video', mode_key, 'Portrait']" in res
    # Ingredients is still a deliberate hold, with its sentence where v945.15 reads it
    assert "UNMEASURED on" in src and "deliberate hold" in src
    assert "page._model_apply_debug = _V962_INGREDIENTS_HOLD" in res
    # the four legacy sites branch BEFORE their Radix/dialog work
    for fn, marker in (("upload_both_frames_with_policy_check", "_v962_attach_frame(page, start_image, 'start'"),
                       ("click_frame_and_upload_with_policy_check", "_v962_attach_frame(page, image_path, which"),
                       ("fill_prompt_textarea", "_v962_type_prompt(page, prompt)"),
                       ("is_generate_button_enabled", "_v962_generate_enabled(page)"),
                       ("click_generate_button", "_V962_GENERATE_BTN"),
                       ("ensure_lower_priority_model", "_v962_on_new_host(page)")):
        body = _func_body(src, fn)
        assert marker in body, fn
    both = _func_body(src, "upload_both_frames_with_policy_check")
    assert both.index("_v962_attach_frame(") < both.index('aria-haspopup="dialog"\']') if 'aria-haspopup="dialog"\']' in both else True
    single = _func_body(src, "click_frame_and_upload_with_policy_check")
    assert single.index("_v962_on_new_host(page)") < single.index("frame_selector = ")


def test_v962_3_new_host_predicate_and_model_normaliser_run_without_a_browser():
    for path in WORKERS:
        src = open(path, encoding="utf-8").read()
        tree = ast.parse(src)
        pieces = [ast.get_source_segment(src, n) for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name in ("_v962_on_new_host", "_v962_norm_model")]
        assert len(pieces) == 2, os.path.basename(path)
        ns = {"re": re}
        exec("\n".join(pieces), ns)

        class _P:
            def __init__(self, u):
                self.url = u

        assert ns["_v962_on_new_host"](_P(NEW_PROJECT))
        assert not ns["_v962_on_new_host"](_P(OLD_PROJECT))
        assert not ns["_v962_on_new_host"](_P(None))
        assert ns["_v962_norm_model"]("Veo 3.1 - Lite [Lower Priority]") == "veo 3.1 - lite"
        assert ns["_v962_norm_model"]("  Omni   Flash ") == "omni flash"
