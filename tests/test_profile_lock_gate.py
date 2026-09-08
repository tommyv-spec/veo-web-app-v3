"""The profile-lock gate: never launch a browser into a profile someone holds.

Why this exists. An orphan Camoufox keeps holding its `-profile` directory after
its worker dies, and every launch route in this repo used to walk straight into
it. Measured 2026-09-08 on a scratch profile: the second launch spends 125
seconds and then raises `TargetClosedError`, and on a desktop it also puts the
modal "Camoufox is already running, but is not responding" on the operator's
screen. Headless does not help — the camoufox processes measured that day
carried `-headless` and the dialog appeared anyway.

The cleanup itself was already proven in `tools/launch_workers.py`
(`clear_flow_browser`), but it only ran on ONE launch route, so a direct
`python flow_worker.py`, an `fg_launch_*.ps1` helper or the .bat all skipped it.
`docs/handoff-archive/2026-08.md:3949` recorded the missing piece on 2026-08-17
and it stayed open: "Wants a startup sweep of the lock when no live process
holds the profile." These tests pin that sweep to `launch_context`, the one
function every worker and every tool passes through.

No browser is launched here and the operator's profile is never touched.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

STATIC = Path(__file__).resolve().parents[1] / "static"
if str(STATIC) not in sys.path:
    sys.path.insert(0, str(STATIC))

import browser_driver as bd  # noqa: E402

TARGET = r"C:\Users\t\veo-worker\firefox-session"
CAMOUFOX = r"C:\camoufox\camoufox.exe"


def _row(pid, ppid, name, cmd):
    return {"pid": pid, "ppid": ppid, "name": name, "cmd": cmd}


def _holder_rows():
    """A live orphan on TARGET, plus a DIFFERENT lane that must be left alone.

    The second lane is the `-2` suffix trap from v684: a substring match on
    "firefox-session" also matches "firefox-session-2", so a careless sweep
    kills another account's browser. It is here to be a mutation check.
    """
    return [
        # the orphan holding TARGET: parent carries -profile, child does not
        _row(100, 900, "camoufox.exe", f'"{CAMOUFOX}" -no-remote -headless -profile {TARGET}'),
        _row(101, 100, "camoufox.exe", f'"{CAMOUFOX}" -contentproc --channel=1'),
        # a different lane, must never be touched
        _row(200, 901, "camoufox.exe", f'"{CAMOUFOX}" -no-remote -profile {TARGET}-2'),
        _row(201, 200, "camoufox.exe", f'"{CAMOUFOX}" -contentproc --channel=2'),
    ]


class ParseProfileArg(unittest.TestCase):
    def test_firefox_profile_flag(self):
        cmd = f'"{CAMOUFOX}" -no-remote -headless -profile {TARGET}'
        self.assertEqual(TARGET.lower(),
                         (bd.profile_arg_from_cmdline(cmd) or "").lower())

    def test_quoted_path_with_spaces(self):
        p = r"C:\Users\t\My Worker\firefox-session"
        self.assertEqual(p.lower(),
                         (bd.profile_arg_from_cmdline(f'camoufox.exe -profile "{p}"') or "").lower())

    def test_chromium_user_data_dir(self):
        cmd = f'chrome.exe --user-data-dir={TARGET} --no-sandbox'
        self.assertEqual(TARGET.lower(),
                         (bd.profile_arg_from_cmdline(cmd) or "").lower())

    def test_content_process_has_no_profile(self):
        """Children carry no -profile. They are reached through ppid, never by parsing."""
        self.assertIsNone(bd.profile_arg_from_cmdline(f'"{CAMOUFOX}" -contentproc --channel=1'))


class Holders(unittest.TestCase):
    def test_finds_parent_and_child(self):
        pids = [h["pid"] for h in bd.profile_holders(TARGET, rows=_holder_rows())]
        self.assertIn(100, pids)
        self.assertIn(101, pids)

    def test_does_not_match_a_longer_profile_name(self):
        """THE mutation check: firefox-session must not match firefox-session-2."""
        pids = [h["pid"] for h in bd.profile_holders(TARGET, rows=_holder_rows())]
        self.assertNotIn(200, pids)
        self.assertNotIn(201, pids)

    def test_a_free_profile_has_no_holders(self):
        self.assertEqual([], bd.profile_holders(r"C:\somewhere\else", rows=_holder_rows()))


class Gate(unittest.TestCase):
    """ensure_profile_unlocked: free it, or refuse to launch. Never launch into a lock."""

    def setUp(self):
        self.killed = []
        self.kill_patch = mock.patch.object(bd, "_kill_pid",
                                            side_effect=lambda pid: self.killed.append(pid) or True)
        self.kill_patch.start()
        self.addCleanup(self.kill_patch.stop)

    def _gate(self, held_sequence, rows):
        """held_sequence: what lock_is_held returns on each successive probe."""
        seq = list(held_sequence)
        with mock.patch.object(bd, "lock_is_held", side_effect=lambda d: seq.pop(0) if seq else False), \
             mock.patch.object(bd, "_query_processes", return_value=rows):
            return bd.ensure_profile_unlocked(TARGET, log=lambda *_a, **_k: None, settle_s=0)

    def test_free_profile_launches_and_kills_nothing(self):
        self.assertTrue(self._gate([False], _holder_rows()))
        self.assertEqual([], self.killed)

    def test_orphan_is_cleared_children_before_parents(self):
        """No live python ancestor (pid 900 is gone) -> the holder is an orphan -> kill it."""
        self.assertTrue(self._gate([True, False], _holder_rows()))
        self.assertEqual([101, 100], self.killed,
                         "children must die before their parent, and only this profile's")

    def test_a_live_worker_that_owns_the_profile_is_never_killed(self):
        """Another lane's LIVE worker owns it: refuse the launch, do not murder its browser."""
        rows = _holder_rows() + [_row(900, 1, "python.exe", "python.exe flow_worker.py --single")]
        with self.assertRaises(bd.ProfileLockedError) as caught:
            self._gate([True, True, True, True], rows)
        self.assertEqual([], self.killed, "a live owner's browser must not be killed")
        self.assertIn("900", str(caught.exception), "the message must name the owning process")

    def test_our_own_stale_browser_is_ours_to_kill(self):
        """A recovery relaunch inside the SAME worker must clear its own browser."""
        rows = _holder_rows() + [_row(900, 1, "python.exe", "python.exe flow_worker.py --single")]
        with mock.patch.object(bd.os, "getpid", return_value=900):
            self.assertTrue(self._gate([True, False], rows))
        self.assertEqual([101, 100], self.killed)

    def test_refuses_when_the_lock_will_not_clear(self):
        """Cannot free it -> raise. Launching anyway is the 125s hang plus the modal."""
        with self.assertRaises(bd.ProfileLockedError):
            self._gate([True, True, True, True, True], _holder_rows())


class ProcessQuery(unittest.TestCase):
    """A query that quietly returns less than reality is the bug, not the fallback.

    Measured on this box 2026-09-08: PowerShell's ConvertTo-Json emits raw
    control characters that appear inside real Windows command lines, and a
    strict json.loads then rejects the entire 270 KB payload over one byte
    ("Invalid control character at line 1 column 100853"). The first cut of this
    gate swallowed that and reported zero holders while two camoufox trees ran.
    """

    def _powershell(self, stdout, rc=0, stderr=""):
        done = mock.MagicMock(stdout=stdout, stderr=stderr, returncode=rc)
        return mock.patch.object(bd.subprocess, "run", return_value=done)

    @unittest.skipUnless(os.name == "nt", "windows query path")
    def test_control_character_in_a_command_line_still_parses(self):
        payload = ('[{"ProcessId":100,"ParentProcessId":900,"Name":"camoufox.exe",'
                   '"CommandLine":"camoufox.exe -profile C:\\\\p\tX"}]')
        with self._powershell(payload):
            rows = bd._query_processes()
        self.assertEqual(1, len(rows))
        self.assertEqual(100, rows[0]["pid"])

    @unittest.skipUnless(os.name == "nt", "windows query path")
    def test_an_unreadable_process_list_raises_instead_of_reading_as_empty(self):
        with self._powershell("", rc=1, stderr="access denied"):
            with self.assertRaises(bd.ProcessQueryError):
                bd._query_processes()

    def test_a_failed_query_refuses_the_launch_rather_than_launching_blind(self):
        with mock.patch.object(bd, "lock_is_held", return_value=True), \
             mock.patch.object(bd, "_query_processes",
                               side_effect=bd.ProcessQueryError("boom")):
            with self.assertRaises(bd.ProfileLockedError):
                bd.ensure_profile_unlocked(TARGET, log=lambda *_a, **_k: None, settle_s=0)


class SiblingWorkerInTheSameProcess(unittest.TestCase):
    """The multi-account coordinator runs several AccountWorkers as threads of
    ONE python process, so os.getpid() cannot tell "my own leftover browser"
    from "my sibling's live one". Killing on a pid match takes a live render
    out mid-flight. An OPEN context registers its profile; that is the
    difference between the two.
    """

    def setUp(self):
        bd._OPEN_PROFILES.clear()
        self.addCleanup(bd._OPEN_PROFILES.clear)
        self.killed = []
        p = mock.patch.object(bd, "_kill_pid",
                              side_effect=lambda pid: self.killed.append(pid) or True)
        p.start()
        self.addCleanup(p.stop)

    def _rows(self):
        return _holder_rows() + [_row(900, 1, "python.exe", "python.exe flow_worker.py")]

    def _gate(self, held):
        seq = list(held)
        with mock.patch.object(bd, "lock_is_held", side_effect=lambda d: seq.pop(0) if seq else False), \
             mock.patch.object(bd, "_query_processes", return_value=self._rows()), \
             mock.patch.object(bd.os, "getpid", return_value=900):
            return bd.ensure_profile_unlocked(TARGET, log=lambda *_a, **_k: None, settle_s=0)

    def test_a_sibling_with_an_open_context_is_refused_not_killed(self):
        ctx = mock.MagicMock()
        bd._register_open_profile(TARGET, ctx)
        with self.assertRaises(bd.ProfileLockedError):
            self._gate([True, True, True, True])
        self.assertEqual([], self.killed, "a sibling's live browser must survive")

    def test_our_own_leftover_after_the_context_closed_is_killed(self):
        ctx = mock.MagicMock()
        bd._register_open_profile(TARGET, ctx)
        # fire the close handler playwright would fire
        ctx.on.call_args[0][1]()
        self.assertFalse(bd.profile_open_in_this_process(TARGET))
        self.assertTrue(self._gate([True, False]))
        self.assertEqual([101, 100], self.killed)

    def test_a_context_with_no_close_event_does_not_pin_the_profile_forever(self):
        ctx = mock.MagicMock()
        ctx.on.side_effect = RuntimeError("no close event here")
        bd._register_open_profile(TARGET, ctx)
        self.assertFalse(bd.profile_open_in_this_process(TARGET),
                         "otherwise every later relaunch in this process is refused")


class PosixSettleLoop(unittest.TestCase):
    """`not lock_is_held(...)` is True for posix's None, so the loop declared the
    profile free before the killed processes had gone. The process list is the
    verdict where the lock cannot be probed."""

    def setUp(self):
        bd._OPEN_PROFILES.clear()
        self.addCleanup(bd._OPEN_PROFILES.clear)

    def test_none_from_the_probe_is_not_read_as_free(self):
        rows = _holder_rows()
        with mock.patch.object(bd, "lock_is_held", return_value=None), \
             mock.patch.object(bd, "_query_processes", return_value=rows), \
             mock.patch.object(bd, "_kill_pid", return_value=True), \
             mock.patch.object(bd, "profile_holders", side_effect=[
                 [{"pid": 100, "ppid": 900, "name": "camoufox.exe", "cmd": "x"}],  # holders to kill
                 [{"pid": 100, "ppid": 900, "name": "camoufox.exe", "cmd": "x"}],  # still there
                 [{"pid": 100, "ppid": 900, "name": "camoufox.exe", "cmd": "x"}],
                 [{"pid": 100, "ppid": 900, "name": "camoufox.exe", "cmd": "x"}],
                 [{"pid": 100, "ppid": 900, "name": "camoufox.exe", "cmd": "x"}]]):
            with self.assertRaises(bd.ProfileLockedError):
                bd.ensure_profile_unlocked(TARGET, log=lambda *_a, **_k: None, settle_s=0)

    def test_none_plus_an_empty_holder_list_is_free(self):
        with mock.patch.object(bd, "lock_is_held", return_value=None), \
             mock.patch.object(bd, "_query_processes", return_value=_holder_rows()), \
             mock.patch.object(bd, "_kill_pid", return_value=True), \
             mock.patch.object(bd, "profile_holders", side_effect=[
                 [{"pid": 100, "ppid": 0, "name": "camoufox.exe", "cmd": "x"}], []]):
            self.assertTrue(bd.ensure_profile_unlocked(TARGET, log=lambda *_a, **_k: None,
                                                       settle_s=0))


class LockProbe(unittest.TestCase):
    """The probe is a delete attempt: Windows refuses it while a browser holds it.

    Measured 2026-09-08 with a real Camoufox on a scratch profile:
      held  -> PermissionError "used by another process"
      closed-> deleted
    """

    def test_absent_lock_reads_as_free(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(bd.lock_is_held(d))

    def test_deletable_lock_reads_as_free_and_is_removed(self):
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / "parent.lock"
            lock.write_text("")
            self.assertFalse(bd.lock_is_held(d))
            self.assertFalse(lock.exists(), "a stale lock is swept, not left to fail the next launch")

    @unittest.skipUnless(os.name == "nt", "the delete probe is only honest on Windows")
    def test_open_exclusive_lock_reads_as_held(self):
        import msvcrt
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / "parent.lock"
            lock.write_text("")
            fh = os.open(str(lock), os.O_RDWR | os.O_BINARY)
            try:
                msvcrt.locking(fh, msvcrt.LK_NBLCK, 1)
                self.assertTrue(bd.lock_is_held(d))
            finally:
                os.close(fh)

    @unittest.skipIf(os.name == "nt", "posix only")
    def test_posix_never_uses_the_delete_probe(self):
        """On posix an unlink succeeds even while Firefox holds it, so deleting
        would destroy a live profile's lock and report 'free'. Enumerate instead."""
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / ".parentlock"
            lock.write_text("")
            bd.lock_is_held(d)
            self.assertTrue(lock.exists())


class WiredIntoLaunch(unittest.TestCase):
    """The gate is worthless unless the one function every route calls runs it."""

    def test_launch_context_gates_firefox(self):
        seen = []
        with mock.patch.object(bd, "ensure_profile_unlocked", side_effect=lambda d, **k: seen.append(d)), \
             mock.patch.object(bd, "camoufox_launch_kwargs", side_effect=lambda k, **kw: k), \
             mock.patch.dict(sys.modules, {"camoufox": mock.MagicMock(),
                                           "camoufox.sync_api": mock.MagicMock()}):
            bd.launch_context(mock.MagicMock(), "firefox", user_data_dir=TARGET)
        self.assertEqual([TARGET], seen)

    def test_launch_context_without_a_profile_does_not_gate(self):
        seen = []
        with mock.patch.object(bd, "ensure_profile_unlocked", side_effect=lambda d, **k: seen.append(d)), \
             mock.patch.object(bd, "camoufox_launch_kwargs", side_effect=lambda k, **kw: k), \
             mock.patch.dict(sys.modules, {"camoufox": mock.MagicMock(),
                                           "camoufox.sync_api": mock.MagicMock()}):
            bd.launch_context(mock.MagicMock(), "firefox")
        self.assertEqual([], seen)


if __name__ == "__main__":
    unittest.main()
