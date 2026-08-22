"""v939.7 — catch a clip that CANNOT say its line before it renders.

The cheapest defect to fix is the one that never renders. `clip_qc.py` listens
to finished clips and finds the ones that were cut short; by then Veo has
already spent the render. This module answers the same question from two
numbers in the database, before a single frame exists:

    is this clip's render window shorter than its own line needs?

That is knowable with no audio, no model and no network beyond one metadata
read, because the platform already owns both halves — `clips.veo_render_
duration_s` (what it will render at) and `clip_duration.pick_clip_duration_
for_line()` (what the v861/v884 table says the line needs).

WHY THIS EXISTS, measured over 509 real clips (2026-08-23):

    stored duration disagrees with the table   96  (18.9%)
      of those, stored is LONGER than needed   82  harmless, but paid for
      of those, stored is SHORTER than needed   9  set up to be cut

Nine clips were guaranteed to lose words before anyone pressed render:

    clip 14303  stored 4s, needs 8s   "this batch sells out fast, so follow..."
    clip 14257  stored 6s, needs 10s  "breaking news in new york. a video..."

Two of those (14302, 14303) later turned up in the cut-clip list that started
this whole line of work. They did not have to.


WHAT IT DOES NOT DO
===================
- It does not touch a clip that is OVER-bound. Rendering a 5-word line in an
  8s window is not a defect; it is some wasted render time and some trailing
  silence the export already trims. Changing it would alter the pacing of
  clips that are fine, to save money nobody asked to save. Reported, never
  changed.
- It does not touch a clip that has already rendered. Pre-flight is
  prevention; a finished clip is `clip_qc.py`'s problem, and widening it
  changes nothing until something re-renders it.
- It does not fix anything unless `--fix` is typed. Default is a report.

A NULL `veo_render_duration_s` is not missing data — it means "use the
job-level duration" (v861: adaptive_duration OFF stores NULL deliberately, and
NULL already means job-level on both render paths). So the job's own duration
is what gets compared for those, not a guess.

CLI
    python code/preflight_duration.py --job <id>
    python code/preflight_duration.py --since-days 7 --limit 20
    python code/preflight_duration.py --job <id> --fix
"""
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Statuses where a render is still AHEAD of the clip, so widening the window
# now actually prevents something. A completed clip is not pre-flight.
PRE_RENDER_STATUSES = ("pending", "waiting_approval")

# Cap on how many clips one --fix run may change, so a first run cannot sweep
# an account. Raise deliberately.
DEFAULT_FIX_CAP = 25


# ============================================================================
# Pure core — two numbers and a comparison. No network, no audio, no models.
# ============================================================================

def effective_duration(clip: Dict[str, Any],
                       job_duration: Optional[int]) -> Optional[int]:
    """What this clip will ACTUALLY render at.

    NULL on the clip is not unknown — v861 stores NULL to mean "use the
    job-level duration", and both render paths already read it that way. So
    the honest answer for a NULL clip is the job's duration, and only a job
    whose own duration we could not read is genuinely unknown.
    """
    stored = clip.get("veo_render_duration_s")
    if stored is not None:
        try:
            return int(stored)
        except (TypeError, ValueError):
            return None
    if job_duration is None:
        return None
    try:
        return int(job_duration)
    except (TypeError, ValueError):
        return None


def check_clip(clip: Dict[str, Any],
               job_duration: Optional[int] = None) -> Dict[str, Any]:
    """Will this clip's window fit its line? Pure.

    verdict:
      under    the window is SHORTER than the line needs - it will be cut
      ok       the window matches what the table asks for
      over     the window is longer than needed - not a defect, just paid for
      unknown  no line, or no duration we could resolve
    """
    from clip_duration import pick_clip_duration_for_line

    line = (clip.get("dialogue_text") or "").strip()
    will_render_at = effective_duration(clip, job_duration)

    if not line or will_render_at is None:
        return {"verdict": "unknown", "needs": None,
                "will_render_at": will_render_at,
                "source": "clip" if clip.get("veo_render_duration_s") is not None
                          else "job",
                "shortfall_s": None}

    needs = int(pick_clip_duration_for_line(line))
    if will_render_at < needs:
        verdict = "under"
    elif will_render_at > needs:
        verdict = "over"
    else:
        verdict = "ok"

    return {
        "verdict": verdict,
        "needs": needs,
        "will_render_at": will_render_at,
        # Which number was used, so a surprising result is traceable to the
        # clip's own column or to the job-level fallback.
        "source": "clip" if clip.get("veo_render_duration_s") is not None else "job",
        "shortfall_s": max(0, needs - will_render_at),
    }


def is_fixable(clip: Dict[str, Any], result: Dict[str, Any]) -> bool:
    """May we widen this clip now, and would it accomplish anything?

    Only UNDER-bound clips, and only while a render is still ahead of them.
    Widening a finished clip changes nothing until something re-renders it,
    and widening an over-bound one would alter pacing that is not broken.
    """
    if result.get("verdict") != "under":
        return False
    return (clip.get("status") or "") in PRE_RENDER_STATUSES


def summarise(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Counts by verdict, plus the two that actually matter."""
    out = {"clips": len(rows), "under": 0, "ok": 0, "over": 0, "unknown": 0,
           "fixable_now": 0, "under_already_rendered": 0,
           "wasted_seconds": 0}
    for r in rows:
        v = r["check"]["verdict"]
        out[v] = out.get(v, 0) + 1
        if v == "under":
            if r["fixable"]:
                out["fixable_now"] += 1
            else:
                out["under_already_rendered"] += 1
        elif v == "over":
            out["wasted_seconds"] += (r["check"]["will_render_at"]
                                      - r["check"]["needs"])
    return out


# ============================================================================
# Network — reuses clip_qc's auth so both CLIs cannot disagree about the token
# ============================================================================

def _qc():
    import clip_qc
    return clip_qc


def fetch_job_duration(session: Any, base: str, job_id: str) -> Optional[int]:
    """The job-level render length, for clips whose own column is NULL.

    Returns None rather than raising: a job whose config we cannot read leaves
    its NULL clips `unknown`, which is the honest answer.
    """
    q = _qc()
    try:
        resp = session.get(q._url(base, f"/api/jobs/{job_id}/config"), timeout=120)
        if resp.status_code != 200:
            return None
        cfg = resp.json()
        conf = cfg.get("config", cfg) if isinstance(cfg, dict) else {}
        raw = conf.get("duration")
        return int(raw) if raw is not None else None
    except Exception:
        return None


def scan_job(session: Any, base: str, job_id: str) -> List[Dict[str, Any]]:
    q = _qc()
    clips = q.fetch_clips(session, base, job_id)
    job_duration = fetch_job_duration(session, base, job_id)
    rows = []
    for clip in clips:
        # A clip with no line to say has no window to get wrong.
        if not (clip.get("dialogue_text") or "").strip():
            continue
        if (clip.get("clip_role") or "") == "visual_pair":
            continue
        check = check_clip(clip, job_duration)
        rows.append({
            "job_id": job_id,
            "clip_id": clip.get("id"),
            "clip_index": clip.get("clip_index"),
            "status": clip.get("status"),
            "line": (clip.get("dialogue_text") or "")[:90],
            "check": check,
            "fixable": is_fixable(clip, check),
        })
    return rows


# ============================================================================
# CLI
# ============================================================================

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="preflight_duration.py",
        description=("Find clips whose render window is shorter than their own "
                     "line needs - before they render. Reports by default; "
                     "--fix widens the ones that have not rendered yet."))
    parser.add_argument("--job", action="append", default=[])
    parser.add_argument("--since-days", type=int, default=None)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--fix", action="store_true",
                        help="widen the UNDER-bound clips that have not rendered yet")
    parser.add_argument("--max", type=int, default=DEFAULT_FIX_CAP, dest="max_fix")
    parser.add_argument("--show-over", action="store_true",
                        help="also list the over-bound clips (not a defect)")
    parser.add_argument("--token", default=None)
    parser.add_argument("--url", default=None)
    args = parser.parse_args(argv)

    q = _qc()
    base = (args.url or q._default_base_url()).rstrip("/")
    try:
        session = q._auth_session(args.token)
    except Exception as exc:
        print(f"[preflight] {exc}", flush=True)
        return 3

    job_ids = list(args.job)
    if not job_ids:
        window = args.since_days if args.since_days is not None else 30
        jobs = q.fetch_jobs(session, base, limit=max(args.limit, 1),
                            since_days=window)
        job_ids = [j.get("id") for j in jobs if j.get("id")][:args.limit]
        print(f"[preflight] no --job given; taking the {len(job_ids)} most recent "
              f"job(s) from the last {window or 'all'} days", flush=True)

    rows: List[Dict[str, Any]] = []
    for job_id in job_ids:
        try:
            rows.extend(scan_job(session, base, job_id))
        except Exception as exc:
            print(f"[preflight] job {job_id}: {exc}", flush=True)

    stats = summarise(rows)
    print(f"\n=== RENDER WINDOW vs WHAT THE LINE NEEDS ===")
    print(f"  clips checked            {stats['clips']}")
    print(f"  window too SHORT         {stats['under']}"
          f"   <-- these will be cut")
    print(f"     of those, fixable now {stats['fixable_now']}"
          f"   (still to render)")
    print(f"     already rendered      {stats['under_already_rendered']}"
          f"   (too late - clip_qc.py's problem)")
    print(f"  window matches           {stats['ok']}")
    print(f"  window longer than needed {stats['over']}"
          f"   ({stats['wasted_seconds']}s of render paid for and unused)")
    if stats["unknown"]:
        print(f"  could not tell           {stats['unknown']}")

    under = [r for r in rows if r["check"]["verdict"] == "under"]
    if under:
        print(f"\n  TOO SHORT - each of these is set up to lose words:")
        for r in under:
            c = r["check"]
            mark = "" if r["fixable"] else "   [already rendered]"
            print(f"    clip {r['clip_id']} (job {str(r['job_id'])[:8]} "
                  f"#{r['clip_index']}, {r['status']}) "
                  f"{c['will_render_at']}s but needs {c['needs']}s "
                  f"(short by {c['shortfall_s']}s, from the {c['source']}){mark}")
            print(f"      {r['line']}")

    if args.show_over:
        over = [r for r in rows if r["check"]["verdict"] == "over"]
        if over:
            print(f"\n  longer than needed (NOT a defect, listed for cost only):")
            for r in over[:20]:
                c = r["check"]
                print(f"    clip {r['clip_id']}: {c['will_render_at']}s for a "
                      f"{c['needs']}s line")

    fixable = [r for r in rows if r["fixable"]][:args.max_fix]
    if not fixable:
        print("\n  nothing to fix: no under-bound clip is still waiting to render.")
        return 0

    if not args.fix:
        print(f"\n  {len(fixable)} clip(s) could be widened BEFORE they render. "
              f"Re-run with --fix to do it.")
        return 0

    done = failed = 0
    for r in fixable:
        ok, why = q.set_clip_duration(session, base, r["clip_id"],
                                      r["check"]["needs"])
        print(f"    clip {r['clip_id']}: {why}", flush=True)
        done += ok
        failed += (not ok)
    print(f"\n  {done} widened before rendering, {failed} failed.")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
