"""The lazy R2 export probe must be bounded by a CLOCK, not just by a count.

export_probe.evidence_candidates caps how MANY jobs it probes (LAZY_PROBE_CAP=6).
That is not a time bound. Each probe is an R2 download + ffmpeg/ffprobe, so a
single stalled leg can hold one probe for minutes — and six of them can blow past
gunicorn's `--timeout 300`, which SIGABRTs the worker and kills its in-flight DB
connections (the 2026-07-06 outage shape). The probe runs on the request path,
including the unattended /api/local-videos/rematch poll, so it needs a wall-clock
budget checked BEFORE each probe.

Stopping early is the SAFE failure: an un-probed candidate keeps NULL
duration/fp, and evidence_pick reads NULL as an abstention — so the match falls
back to a manual pick instead of guessing.

The clock is faked (each probe "costs" seconds) so the test is deterministic and
instant rather than actually sleeping.
"""
import inspect

import export_probe


class _FakeJob:
    def __init__(self, jid):
        self.id = jid
        self.export_audio_fp = None      # never probed
        self.export_duration_s = None
        self.export_probed_at = None


class _FakeDB:
    def commit(self):
        pass


class _FakeClock:
    """monotonic() that only moves when a probe says it took time."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def _install(monkeypatch, seconds_per_probe):
    """Patch the clock + both probe fns. Returns (clock, probed_ids)."""
    clock = _FakeClock()
    probed = []
    monkeypatch.setattr(export_probe.time, "monotonic", clock)

    def _slow_fp(db, job):
        probed.append(job.id)
        clock.now += seconds_per_probe          # the stalled R2 leg
        job.export_audio_fp = "fp:" + job.id    # probe succeeded, just slowly
        return job.export_audio_fp

    def _slow_dur(db, job):
        probed.append(job.id)
        clock.now += seconds_per_probe
        job.export_duration_s = 30.0
        return 30.0

    monkeypatch.setattr(export_probe, "ensure_export_fingerprint", _slow_fp)
    monkeypatch.setattr(export_probe, "ensure_export_duration", _slow_dur)
    return clock, probed


def test_budget_stops_probing_before_the_count_cap(monkeypatch):
    """8s per probe against a 20s budget: 3 probes, then stop. NOT all 6."""
    clock, probed = _install(monkeypatch, seconds_per_probe=8.0)
    jobs = [_FakeJob(f"job{i}") for i in range(6)]

    out = export_probe.evidence_candidates(
        _FakeDB(), jobs, priority_ids=[j.id for j in jobs], budget_s=20.0,
    )

    # probe 1 at t=0, probe 2 at t=8, probe 3 at t=16 — at t=24 the budget is
    # spent, so the remaining 3 are skipped even though max_probe allows 6.
    assert probed == ["job0", "job1", "job2"], probed
    assert clock.now == 24.0


def test_budget_skipped_candidates_abstain(monkeypatch):
    """The un-probed candidates still come back — carrying NULL evidence.

    That is what makes stopping early safe: evidence_pick sees None and abstains,
    so the video waits for a human instead of being auto-published on a guess.
    """
    _clock, probed = _install(monkeypatch, seconds_per_probe=8.0)
    jobs = [_FakeJob(f"job{i}") for i in range(6)]

    out = export_probe.evidence_candidates(
        _FakeDB(), jobs, priority_ids=[j.id for j in jobs], budget_s=20.0,
    )

    assert len(out) == 6, "every candidate is still reported"
    by_id = {c["job_id"]: c for c in out}
    for jid in probed:
        assert by_id[jid]["export_audio_fp"] is not None
    for jid in (j.id for j in jobs if j.id not in probed):
        assert by_id[jid]["export_audio_fp"] is None
        assert by_id[jid]["export_duration_s"] is None


def test_zero_budget_probes_nothing(monkeypatch):
    """A sweep that has already burned its budget hands down 0 and probes none."""
    _clock, probed = _install(monkeypatch, seconds_per_probe=8.0)
    jobs = [_FakeJob(f"job{i}") for i in range(3)]

    export_probe.evidence_candidates(
        _FakeDB(), jobs, priority_ids=[j.id for j in jobs], budget_s=0.0,
    )

    assert probed == []


def test_count_cap_still_applies_within_budget(monkeypatch):
    """The budget ADDS to LAZY_PROBE_CAP, it does not replace it."""
    _clock, probed = _install(monkeypatch, seconds_per_probe=0.0)
    jobs = [_FakeJob(f"job{i}") for i in range(10)]

    export_probe.evidence_candidates(
        _FakeDB(), jobs, priority_ids=[j.id for j in jobs], budget_s=20.0,
    )

    assert len(probed) == export_probe.LAZY_PROBE_CAP


def test_sweep_passes_its_remaining_budget_down():
    """local_transcribe's sweep guard is only checked BETWEEN videos, so one
    video's probes must not be allowed to overrun it."""
    import local_transcribe

    sig = inspect.signature(local_transcribe._maybe_auto_match)
    assert "budget_s" in sig.parameters, "_maybe_auto_match takes no budget"

    src = inspect.getsource(local_transcribe.rematch_unmatched)
    assert "budget_s=remaining" in src, "the sweep does not pass its remaining budget down"


def test_probing_handlers_are_not_on_the_event_loop():
    """The handlers that reach R2 must be plain `def` so FastAPI runs them in the
    anyio threadpool. As `async def`, their blocking downloads + ffmpeg freeze the
    single event loop of the single uvicorn worker — every other request stalls,
    and a block past `--timeout 300` gets the worker SIGABRT'd.
    """
    import main

    assert not inspect.iscoroutinefunction(main.suggest_matches)
    assert not inspect.iscoroutinefunction(main.rematch_local_videos)
