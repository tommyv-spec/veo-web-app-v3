"""Regression checks for Flow's Firefox login/profile recovery.

These tests do not launch a browser and never read the operator's profile.
"""
from __future__ import annotations

import ast
import importlib.util
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


STATIC = Path(__file__).resolve().parents[1] / "static"
ROOT = Path(__file__).resolve().parents[2]
FLOW_SOURCE = (STATIC / "flow_worker.py").read_text(encoding="utf-8")
IMAGE_SOURCE = (ROOT / "code" / "image_worker.py").read_text(encoding="utf-8")
PROFILE_TOOL_SOURCE = (ROOT / "tools" / "flow_profile.py").read_text(encoding="utf-8")

_spec = importlib.util.spec_from_file_location(
    "firefox_profile_pull_under_test", STATIC / "firefox_profile_pull.py")
ffpull = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ffpull)


def _function_source(name: str) -> str:
    tree = ast.parse(FLOW_SOURCE)
    node = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                and n.name == name)
    return ast.get_source_segment(FLOW_SOURCE, node)


def _cookie_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("create table moz_cookies (host text, name text, value text)")
    con.execute("insert into moz_cookies values ('.google.com', 'SID', 'not-printed')")
    con.commit()
    con.close()


class ProfileSnapshotSafety(unittest.TestCase):
    def test_flow_project_url_alone_is_not_login_proof(self):
        body = _function_source("ensure_logged_into_flow")
        self.assertNotIn(
            "if is_flow_project(url):\n                return 'flow_logged_in'",
            body,
        )

    # v963.3 — these are the ORIGINAL assertions, re-pointed at
    # _flow_page_state. The login logic is byte-identical to what shipped before
    # 2026-09-07; it only moved to module level so tests can load it by AST.
    # A rewrite that made page text unable to confirm a login was reverted on the
    # operator's call after it stopped two workers with live profiles.

    def test_signed_in_project_controls_are_login_proof(self):
        body = _function_source("_flow_page_state")
        self.assertIn("button[aria-label='Start generation']", body)
        self.assertIn("button[aria-label='Add media menu']", body)
        self.assertIn("a[href*='/project/'][href$='/tools']", body)

    def test_project_api_fallback_requires_real_authenticated_response(self):
        body = _function_source("_flow_page_state")
        self.assertIn('if "/project/" in url:', body)
        self.assertIn('_auth_token = _FA_TOKEN_STORE.token or ""', body)
        self.assertIn("if _auth_token:", body)
        self.assertIn("if not _fa_is_error(_probe):", body)

    def test_cookie_authenticated_init_response_is_bound_to_exact_page(self):
        init = _function_source("_fa_init_project_best_effort")
        login = _function_source("_flow_page_state")
        self.assertIn('page._flow_authenticated_api_proof = "credits"', init)
        self.assertIn('if "/project/" in url and _api_proof:', login)

    def test_project_editor_text_is_visible_dom_login_proof(self):
        body = _function_source("_flow_page_state")
        self.assertIn("['videos', 'scenes', 'escenas']", body)
        self.assertIn("editor && !signedOut && !broken", body)

    def test_flow_dom_auth_publishes_a_pid_bound_ready_marker(self):
        login = _function_source("ensure_logged_into_flow")
        publish = _function_source("_publish_flow_auth_ready")
        self.assertIn("_publish_flow_auth_ready(page, label)", login)
        self.assertIn('"pid": os.getpid()', publish)
        self.assertIn('"authenticated_at": time.time()', publish)
        self.assertIn('getattr(\n                    page, "_flow_auth_proof"', publish)

    def test_login_wall_revokes_this_process_ready_marker(self):
        login = _function_source("ensure_logged_into_flow")
        self.assertIn("_clear_flow_auth_ready()", login)

    def test_flow_refreshes_private_golden_once_per_process(self):
        body = _function_source("_maybe_pull_laptop_profile")
        firefox_branch = body.split("if _bd.is_firefox_mode(BROWSER_MODE):", 1)[1]
        self.assertNotIn("worker_profile_needs_seed", firefox_branch)
        self.assertIn("if golden_folder in _LAPTOP_COPIED_GOLDENS", firefox_branch)
        self.assertIn("build_firefox_golden_from_profile", firefox_branch)

    def test_login_wall_never_copies_the_operator_profile_automatically(self):
        body = _function_source("ensure_logged_into_flow")
        self.assertNotIn("refresh_firefox_session_from_profile(", body)

    def test_login_timeout_cannot_be_returned_as_verified_fresh_login(self):
        body = _function_source("ensure_logged_into_flow")
        self.assertIn("raise FlowLoginRequired", body)
        node = next(n for n in ast.parse(FLOW_SOURCE).body
                    if isinstance(n, ast.FunctionDef)
                    and n.name == "ensure_logged_into_flow")
        self.assertIsInstance(node.body[-1], ast.Raise)

    def test_failed_relaunch_never_restores_the_same_stale_golden(self):
        self.assertNotIn("Golden already exists — keeping original (write-once).", FLOW_SOURCE)
        self.assertNotIn("Login was required after relaunch — golden session was expired.", FLOW_SOURCE)

    def test_worker_profile_pull_only_seeds_empty_private_state(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session = root / "firefox-session"
            golden = root / "firefox-golden"
            self.assertTrue(ffpull.worker_profile_needs_seed(session, golden))
            session.mkdir()
            (session / "prefs.js").write_text("private state", encoding="utf-8")
            self.assertFalse(ffpull.worker_profile_needs_seed(session, golden))
            (session / "prefs.js").unlink()
            golden.mkdir()
            self.assertFalse(ffpull.worker_profile_needs_seed(session, golden))

    def test_sqlite_backup_failure_keeps_existing_golden(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "source"
            src.mkdir()
            (src / "cookies.sqlite").write_bytes(b"live-db")
            golden = root / "golden"
            golden.mkdir()
            (golden / "sentinel").write_text("old-good", encoding="utf-8")
            real_connect = ffpull.sqlite3.connect

            def fail_readonly_backup(database, *args, **kwargs):
                if isinstance(database, str) and database.startswith("file:"):
                    raise sqlite3.OperationalError("forced backup failure")
                return real_connect(database, *args, **kwargs)

            with mock.patch.object(ffpull, "locate_firefox_profile",
                                   return_value=str(src)), \
                 mock.patch.object(ffpull.sqlite3, "connect",
                                   side_effect=fail_readonly_backup):
                ok = ffpull.build_firefox_golden_from_profile(
                    "not-printed", str(golden), account_num=1, log=lambda _m: None)
            self.assertFalse(ok)
            self.assertEqual("old-good", (golden / "sentinel").read_text(encoding="utf-8"))
            self.assertFalse(Path(str(golden) + ".ffpull-tmp").exists())

    def test_atomic_golden_swap_rolls_back_if_publish_fails(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            staged = root / "golden.ffpull-tmp"
            staged.mkdir()
            (staged / "new").write_text("new", encoding="utf-8")
            golden = root / "golden"
            golden.mkdir()
            (golden / "old").write_text("old", encoding="utf-8")
            real_replace = os.replace

            def fail_publish(src, dst):
                if Path(src) == staged and Path(dst) == golden:
                    raise OSError("forced publish failure")
                return real_replace(src, dst)

            with mock.patch.object(ffpull.os, "replace", side_effect=fail_publish):
                with self.assertRaisesRegex(OSError, "forced publish failure"):
                    ffpull.replace_golden_atomically(str(staged), str(golden))
            self.assertEqual("old", (golden / "old").read_text(encoding="utf-8"))

    def test_all_live_cookie_snapshots_use_sqlite_online_backup(self):
        for name in ("google_cookie_count", "_read_cookies_wal_applied",
                     "_probe_google_emails_inproc"):
            body = ast.get_source_segment(
                (STATIC / "firefox_profile_pull.py").read_text(encoding="utf-8"),
                next(n for n in ast.parse(
                    (STATIC / "firefox_profile_pull.py").read_text(encoding="utf-8")
                ).body if isinstance(n, ast.FunctionDef) and n.name == name))
            self.assertIn("snapshot_sqlite_database", body, name)
            self.assertNotIn("cookies.sqlite-wal", body, name)

    def test_recovery_tool_uses_the_same_snapshot_broker_and_has_flow_target(self):
        tree = ast.parse(PROFILE_TOOL_SOURCE)
        build = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name == "build_minimal")
        body = ast.get_source_segment(PROFILE_TOOL_SOURCE, build)
        self.assertIn("snapshot_firefox_profile", body)
        self.assertNotIn("shutil.copy", body)
        self.assertIn('choices=["swap", "image", "flow"]', PROFILE_TOOL_SOURCE)

    def test_disabled_image_pull_still_allows_one_empty_profile_seed(self):
        tree = ast.parse(IMAGE_SOURCE)
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "_maybe_pull_laptop_profile")
        body = ast.get_source_segment(IMAGE_SOURCE, node)
        disabled = body.index("LAPTOP_PULL_DISABLED")
        empty_check = body.index("worker_profile_needs_seed")
        self.assertLess(empty_check, disabled)

    def test_explicit_live_refresh_obeys_pull_disable(self):
        body = _function_source("refresh_firefox_session_from_profile")
        self.assertIn("LAPTOP_PULL_DISABLED", body)

    def test_empty_seed_failure_stops_before_browser_launch(self):
        for source in (FLOW_SOURCE, IMAGE_SOURCE):
            tree = ast.parse(source)
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                        and n.name == "_maybe_pull_laptop_profile")
            body = ast.get_source_segment(source, node)
            self.assertIn("raise FirefoxProfileSourceUnavailable", body)
            self.assertIn("hold_flow_backed_lanes", body)

    def test_image_broker_import_failure_is_fail_closed(self):
        tree = ast.parse(IMAGE_SOURCE)
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "_maybe_pull_laptop_profile")
        outer_try = next(item for item in node.body if isinstance(item, ast.Try))
        generic = next(
            handler for handler in outer_try.handlers
            if handler.type is not None
            and ast.get_source_segment(IMAGE_SOURCE, handler.type) == "Exception"
        )
        body = ast.get_source_segment(IMAGE_SOURCE, generic)
        self.assertIn("if FIREFOX_MODE", body)
        self.assertIn("hold_flow_backed_lanes", body)
        self.assertIn("raise FirefoxProfileSourceUnavailable", body)


if __name__ == "__main__":
    unittest.main()


class LoginCheckIsBounded(unittest.TestCase):
    """v963.5 — the login VERDICT is the months-old one; the CALL has a deadline.

    These are separate concerns and conflating them cost a night. The verdict
    (editor text confirms, splash and error page veto) is deliberately unchanged
    and every assertion above still proves it. What changed is that its
    `page.evaluate` became a `wait_for_function` with a timeout, because
    `page.evaluate` has none and this call runs right after a reload while the
    JS context is being replaced.

    Measured twice on the primary worker, 2026-09-07/08: 8 hours stuck on clip
    37, then 21 minutes on clip 4. Both times the process was alive, the
    heartbeat still reported online, py-spy showed MainThread parked in
    run_until_complete waiting on a browser response that never arrived, and the
    stage telemetry stopped dead on `pre_submit_auth_check`.
    """

    def test_the_editor_text_probe_has_a_deadline(self):
        body = _function_source("_flow_page_state")
        self.assertNotIn(".evaluate(", body,
                         "page.evaluate cannot time out; this call hangs the worker")
        self.assertIn("wait_for_function(", body)
        self.assertIn("timeout=", body)

    def test_the_probe_returns_an_object_not_a_bare_boolean(self):
        """wait_for_function waits for TRUTHY, so a real `false` must not block.

        Returning the boolean directly would turn a fast "no, this is not the
        editor" into a full timeout — a correct verdict delivered slowly, which
        on 80 clips is its own outage.
        """
        body = _function_source("_flow_page_state")
        self.assertIn("{ready:", body)
        self.assertIn('.json_value().get("ready")', body)

    def test_the_verdict_markers_are_untouched(self):
        body = _function_source("_flow_page_state")
        for marker in ("['videos', 'scenes', 'escenas']",
                       "create with flow", "something went wrong",
                       "se produjo un error"):
            self.assertIn(marker, body, f"{marker!r} — the verdict must not change")
