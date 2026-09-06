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
