"""A stalled worker must notice itself, hand the job back, and exit.

Why this file exists. On 2026-09-12 the Flow worker stalled three times in one
afternoon and each stall held its lane until a human looked: seven minutes at
104 % of one core, then ten minutes parked at ~0 % CPU, then six more. The
existing `_install_stall_watchdog` only prints, and its stack-diff detector
catches just one of those three shapes. Separately, every force-kill left a
claim on the platform that blocked the next run for ten minutes -- measured at
22, then 12, then 30 consecutive `JOB_NOT_CLAIMABLE` refusals with no Generate
click on any of them, which from the log's side is indistinguishable from a run
that is working.

Four of the tests below exist because Codex found the corresponding bug in the
PLAN, before a line shipped. Each is marked. They are the interesting ones: all
four looked correct while being wrong, and two of them would have caused damage
rather than just failing to help.
"""
import importlib.util
import pathlib
import sys
import threading

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_v978", _STATIC / "flow_worker.py",
)


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------- the detector


def test_liveness_reports_seconds_since_the_last_main_thread_output():
    fw = _load()
    clock = {"t": 1000.0}
    lv = fw._V978Liveness(warn_s=10, act_s=20, now=lambda: clock["t"])
    lv.stamp()
    assert lv.silent_for() == 0.0
    assert lv.level() == "ok"
    clock["t"] += 15
    assert lv.level() == "warn"
    clock["t"] += 10
    assert lv.level() == "act"


def test_a_background_thread_cannot_refresh_liveness():
    """Codex pass-1 finding 3.

    This worker starts background threads for downloads, the heartbeat and the
    response capture. A process-wide detector would let any of them keep the
    watchdog happy while the main job sat frozen -- going quiet exactly when it
    is needed. So only the main thread may prove progress.
    """
    fw = _load()
    clock = {"t": 1000.0}
    lv = fw._V978Liveness(warn_s=10, act_s=20, now=lambda: clock["t"])
    lv.stamp()
    clock["t"] += 25
    t = threading.Thread(target=lv.stamp)
    t.start()
    t.join()
    assert lv.level() == "act", "a background thread must not prove liveness"


def test_the_clock_is_monotonic_by_default():
    """A system clock change must not fake progress or fake a stall."""
    import time as _time
    fw = _load()
    lv = fw._V978Liveness()
    assert lv._now is _time.monotonic


# ------------------------------------------------------------ the escalation


def test_the_warning_does_not_reset_the_silence_clock():
    """Codex pass-1 finding 2 — the one that would have disabled the feature.

    Every main-thread write refreshes the stamp. If the watchdog's own warning
    went through that same wrapped stdout, silence would return to zero on each
    warn and `act` would be unreachable: it would warn forever and never act.
    """
    fw = _load()
    clock = {"t": 1000.0}
    acted = []
    lv = fw._V978Liveness(warn_s=10, act_s=20, now=lambda: clock["t"])
    lv.stamp()

    clock["t"] += 12
    assert fw._v978_tick(lv, act_fn=lambda: acted.append(1)) == "warn"
    assert acted == []

    clock["t"] += 12                      # 24s of silence in total
    assert fw._v978_tick(lv, act_fn=lambda: acted.append(1)) == "act"
    assert acted == [1], "the warning must not have reset the clock"


def test_escalation_fires_once_not_on_every_tick():
    fw = _load()
    clock = {"t": 1000.0}
    acted = []
    lv = fw._V978Liveness(warn_s=10, act_s=20, now=lambda: clock["t"])
    lv.stamp()
    clock["t"] += 30
    for _ in range(4):
        fw._v978_tick(lv, act_fn=lambda: acted.append(1))
    assert acted == [1]


def test_act_releases_the_job_unorphans_then_exits(monkeypatch):
    fw = _load()
    calls = []
    monkeypatch.setattr(
        fw, "_scoped_firstgen_release_parent_jobs",
        lambda reason="": (calls.append(("release", reason)) or ["job-abc"]))
    monkeypatch.setattr(fw, "_v978_unorphan_own_clips",
                        lambda jobs: calls.append(("unorphan", list(jobs))))
    fw._v978_escalate(exit_fn=lambda code: calls.append(("exit", code)))
    assert calls == [("release", "stall-watchdog"),
                     ("unorphan", ["job-abc"]),
                     ("exit", 75)]


def test_the_job_ids_survive_the_release_that_clears_them(monkeypatch):
    """Codex pass-2 finding 1.

    `_scoped_firstgen_release_parent_jobs` copies `_SCOPED_FIRSTGEN_CLAIMED_JOBS`
    and clears it under the lock BEFORE returning the copy. Anything that reads
    the global afterwards sees an empty set and silently does nothing, so the
    ids have to come from the return value.
    """
    fw = _load()
    seen = []
    fw._SCOPED_FIRSTGEN_CLAIMED_JOBS.add("job-abc")
    monkeypatch.setattr(fw, "api_request", lambda *a, **k: {"released": True})
    monkeypatch.setattr(fw, "_v978_unorphan_own_clips",
                        lambda jobs: seen.append(list(jobs)))
    fw._v978_escalate(exit_fn=lambda code: None)
    assert seen == [["job-abc"]], "ids must come from the release's return value"


def test_escalation_still_exits_when_the_release_raises(monkeypatch):
    """A stalled worker must let go of the lane even if the platform is down."""
    fw = _load()
    exits = []

    def boom(reason=""):
        raise RuntimeError("platform down")

    monkeypatch.setattr(fw, "_scoped_firstgen_release_parent_jobs", boom)
    fw._v978_escalate(exit_fn=lambda code: exits.append(code))
    assert exits == [75]


# ------------------------------------------------------- un-orphaning clips


def test_only_the_runs_own_scoped_clips_are_reset(monkeypatch):
    fw = _load()
    posted = []
    monkeypatch.setattr(fw, "FLOW_CLIP_SCOPE_FIRSTGEN", True)
    monkeypatch.setattr(fw, "FLOW_ONLY_CLIP_IDS", [14959])
    monkeypatch.setattr(fw, "api_request",
                        lambda m, p, b=None: posted.append((m, p, b)))
    fw._v978_unorphan_own_clips(["job-abc"])
    assert posted == [("POST", "/clips/14959/status",
                       {"status": "pending", "error_message": None})]


def test_an_unscoped_run_resets_nothing(monkeypatch):
    """Without an allowlist there is no way to know which clips are ours, and
    resetting one another worker is rendering costs a paid render."""
    fw = _load()
    posted = []
    monkeypatch.setattr(fw, "FLOW_CLIP_SCOPE_FIRSTGEN", False)
    monkeypatch.setattr(fw, "FLOW_ONLY_CLIP_IDS", [])
    monkeypatch.setattr(fw, "api_request", lambda *a, **k: posted.append(a))
    fw._v978_unorphan_own_clips(["job-abc"])
    assert posted == []


def test_unorphan_never_raises_into_the_escalation_path(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw, "FLOW_CLIP_SCOPE_FIRSTGEN", True)
    monkeypatch.setattr(fw, "FLOW_ONLY_CLIP_IDS", [14959])

    def boom(*a, **k):
        raise RuntimeError("platform down")

    monkeypatch.setattr(fw, "api_request", boom)
    assert fw._v978_unorphan_own_clips(["job-abc"]) == 0


# ------------------------------------------------- the dead-claim safety rule

DETAIL = {"job_id": "abc123", "job_status": "pending",
          "claimed_by_worker": "worker-BOOK-X-4242"}


def test_a_dead_same_host_claim_is_releasable(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    assert fw._v978_dead_claim_holder(DETAIL) == ("abc123", "worker-BOOK-X-4242")


def test_a_live_pid_is_never_stolen(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: True)
    assert fw._v978_dead_claim_holder(DETAIL) is None


def test_another_host_is_never_stolen(monkeypatch):
    """Two machines sharing a queue must not release each other's claims."""
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    d = dict(DETAIL, claimed_by_worker="worker-OTHER-BOX-4242")
    assert fw._v978_dead_claim_holder(d) is None


def test_the_host_comparison_ignores_case(monkeypatch):
    """Windows reports hostnames in mixed case."""
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "book-x")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    assert fw._v978_dead_claim_holder(DETAIL) is not None


def test_a_hyphenated_hostname_parses(monkeypatch):
    """`worker-` prefix and the host both contain '-', so split from the right."""
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-V1KQL7L6GB")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    d = dict(DETAIL, claimed_by_worker="worker-BOOK-V1KQL7L6GB-519632")
    assert fw._v978_dead_claim_holder(d) == ("abc123",
                                             "worker-BOOK-V1KQL7L6GB-519632")


def test_a_malformed_detail_is_refused(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    for bad in (None, {}, "a string",
                {"job_id": "abc"},
                {"job_id": "", "claimed_by_worker": "worker-BOOK-X-1"},
                {"job_id": 7, "claimed_by_worker": "worker-BOOK-X-1"},
                {"job_id": "abc", "claimed_by_worker": "not-a-worker-id"},
                {"job_id": "abc", "claimed_by_worker": "worker-BOOK-X-notapid"},
                {"job_id": "abc", "claimed_by_worker": "worker-4242"}):
        assert fw._v978_dead_claim_holder(bad) is None, bad


def test_uncertainty_counts_as_alive(monkeypatch):
    """Codex pass-1 finding 1, the fail-closed half.

    When the process list cannot be read we do not know, and "do not know" must
    mean alive: a wrong "dead" verdict steals a live worker's job, which is
    worse than waiting out the server's ten-minute sweep.
    """
    fw = _load()
    monkeypatch.setattr(fw, "_v978_live_pids", lambda: None)
    assert fw._v978_pid_alive(4242) is True
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    assert fw._v978_dead_claim_holder(DETAIL) is None


def test_the_pid_probe_never_signals_the_target():
    """Codex pass-1 finding 1, the destructive half.

    `os.kill(pid, 0)` is NOT a probe on Windows: `signal.CTRL_C_EVENT == 0`, so
    it delivers a Ctrl+C to the target, and any other value routes to
    TerminateProcess. The draft would have killed the live workers it checked,
    so the liveness helpers must not CALL it.

    Checked by parsing rather than by substring: the docstring of
    `_v978_live_pids` deliberately contains the words "os.kill" to warn the next
    reader off it, and a text search cannot tell that warning apart from the
    mistake. (The first version of this test failed on exactly that.)
    """
    import ast

    src = (_STATIC / "flow_worker.py").read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(src)
    targets = {"_v978_live_pids", "_v978_pid_alive", "_v978_dead_claim_holder"}
    checked = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name in targets):
            continue
        checked.add(node.name)
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            fn = inner.func
            name = (f"{getattr(fn.value, 'id', '')}.{fn.attr}"
                    if isinstance(fn, ast.Attribute) else getattr(fn, "id", ""))
            assert name != "os.kill", f"{node.name} calls os.kill"
            assert name != "kill", f"{node.name} calls kill"
    assert checked == targets, f"helpers missing: {targets - checked}"


# ------------------------------------------------------- the self-heal budget


def test_self_heal_is_capped_so_a_claim_war_cannot_loop(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw.socket, "gethostname", lambda: "BOOK-X")
    monkeypatch.setattr(fw, "_v978_pid_alive", lambda pid: False)
    monkeypatch.setattr(fw, "api_request", lambda *a, **k: {"released": True})
    fw._V978_self_heals[0] = 0
    result = {"scope_error_detail": DETAIL}
    fired = [fw._v978_self_heal_dead_claim(result) for _ in range(6)]
    assert fired.count(True) == fw._V978_SELF_HEAL_MAX


def test_self_heal_can_be_switched_off(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw, "_V978_SELF_HEAL_CLAIM", False)
    fw._V978_self_heals[0] = 0
    assert fw._v978_self_heal_dead_claim({"scope_error_detail": DETAIL}) is False
