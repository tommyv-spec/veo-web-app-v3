"""v589 offline motion cross-check — optical flow vs the recorded action.

WHAT THIS IS FOR

The lean `adread.v1` read records what MOVES in each clip. Its named failure
mode is a one-verb summary of a multi-beat clip, and that failure is SILENT:
an artifact where every `action` says "she talks" validates perfectly, because
the schema can only check that the field is a non-empty string.

The obvious check — flag artifacts where few `action` values contain the word
"then" — is the wrong check. It is lossy in both directions (a genuinely
single-beat 1.2s cutaway is CORRECT with one verb; "lifts the jar and twists
the lid, then tips it" is three beats with one "then"), and the moment a
checker greps for a word, the word becomes the target: a later prompt edit
chasing the metric would make the artifact worse while the number improved.
That is the `checkers-verify-declarations-not-decisions` trap.

So this checks EVIDENCE instead of a metric. `motion.json` (v585 Farneback
mean optical flow, already computed for every decode) is a second, independent
source that never saw the prompt and does not care which words the model chose.
When optical flow says the pixels moved a lot and the recorded action says one
verb — or says the frame held still — the two sources CONTRADICT each other.
You cannot make that go away by inserting "then".

WHY ITS OWN SCRIPT, not a mode on v589_video_understanding.py

That module is the decode ENGINE: prompt assembly, provider calls, the response
schema and the handoff validator. Its load-bearing invariant is that the
default `ugc-reel` prompt stays byte-identical, and it is already ~2100 lines.
This is a post-hoc REPORT that consumes two saved files and prints. Different
seam, opposite direction of data flow. Keeping it separate means a heuristic
tweak here can never touch a byte of prompt assembly, and the report can be
imported by tests without importing the engine.

WARN AND PRINT, NEVER RAISE

A slow-cut ad where most clips honestly hold one beat is a VALID artifact.
Hard-failing it would make the honest case unvalidatable — the same trap as the
2026-08-12 forward-only tightening. This exits 0 on every finding. It exits
non-zero only when it cannot read the file it was pointed at.

Usage:
    python v589_motion_cross_check.py <stage4d_vlm.json> [--motion motion.json]
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path

LEAN_SCHEMA_VERSION = "adread.v1"

# The field that carries the motion. `action` is the current lean shape; an
# artifact decoded before the ACTION rework carries `visual` instead, which was
# never ASKED to record motion — so a single-beat reading there is expected and
# says nothing about the current prompt. Reported, clearly labelled, never
# silently treated as the same evidence.
ACTION_FIELD = "action"
LEGACY_ACTION_FIELD = "visual"

# Sequence and list separators only. Deliberately generous: "as" and "while"
# mark simultaneity rather than a new beat and are left out, and any comma or
# "and" is enough to save a value from being called single-beat. The bias is
# toward UNDER-flagging, so a flag means something.
_BEAT_SPLIT = re.compile(r"\bthen\b|\band\b|;|,", re.IGNORECASE)

_NO_MOTION = (
    "held still", "holds still", "no movement", "nothing moves", "no motion",
    "static", "none", "unchanged", "still frame",
)

_UNCHANGED_END = (
    "unchanged", "same as the start", "same as at the start", "no change",
    "identical to the start",
)

# camera_move values that name an actual move. "static" and "locked" are the
# declared no-move answers and are excluded.
_CAMERA_MOVES = (
    "pan", "push in", "push-in", "pull out", "pull-out", "whip", "tilt",
    "drift", "track", "zoom", "dolly", "orbit", "crane", "handheld",
)
_CAMERA_STILL = ("static", "locked", "no movement", "held still", "none")

LONG_CLIP_S = 2.5

# Measured on the real 80-shot ad refurb-competitor-051 with cl100k: the
# pre-rework `visual` field ran 15.5 tokens/shot in the same terse register.
# Printed as SCALE for reading the median, not as a threshold to pass.
REFERENCE_VISUAL_TOKENS_PER_SHOT = 15.5


def _norm(text: object) -> str:
    return " ".join(str(text or "").lower().split()).strip(" .!,;:")


def count_beats(text: object) -> int:
    """How many beats the value reads as. 0 only for an empty value."""
    raw = str(text or "").strip()
    if not raw:
        return 0
    parts = [p for p in _BEAT_SPLIT.split(raw) if p.strip()]
    return max(len(parts), 1)


def is_explicit_no_motion(text: object) -> bool:
    """True when the value says outright that nothing moved.

    Judged on the FIRST clause: "held still, locked off" is still a no-motion
    answer. Anchored to the start so "she holds the jar still while talking"
    and "the static on the screen flickers" do not match.
    """
    norm = _norm(text)
    if not norm:
        return False
    first = re.split(r"[;,]", norm)[0].strip()
    return any(first == phrase or first.startswith(phrase + " ")
               for phrase in _NO_MOTION)


def says_unchanged(text: object) -> bool:
    norm = _norm(text)
    return any(norm.startswith(phrase) for phrase in _UNCHANGED_END)


def names_a_camera_move(text: object) -> bool:
    norm = _norm(text)
    if not norm:
        return False
    if any(norm.startswith(still) for still in _CAMERA_STILL):
        return False
    return any(move in norm for move in _CAMERA_MOVES)


def _token_len(text: object) -> int:
    """cl100k when tiktoken is installed, else a words x 1.33 estimate."""
    raw = str(text or "")
    try:
        import tiktoken
        return len(tiktoken.get_encoding("cl100k_base").encode(raw))
    except Exception:
        return round(len(raw.split()) * 1.33)


def _tokenizer_name() -> str:
    try:
        import tiktoken  # noqa: F401
        return "cl100k"
    except Exception:
        return "words x 1.33 estimate (tiktoken not installed)"


class NotALeanArtifact(ValueError):
    """The report is lean-lane only; the heavy lane has motion_cross_check."""


def load_artifact(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    version = data.get("schema_version")
    if version != LEAN_SCHEMA_VERSION:
        raise NotALeanArtifact(
            f"{path.name} declares schema_version {version!r}, not "
            f"{LEAN_SCHEMA_VERSION!r}. This report reads the LEAN fbads lane "
            f"only - the stage4d.v2 lane already carries a per-shot "
            f"motion_cross_check block, so there is nothing for this to add.")
    return data


def cross_check(artifact: dict, motion: list) -> dict:
    """Join the two sources and collect contradictions. Pure; no I/O."""
    shots = artifact.get("shots") or []
    by_shot = {m.get("shot"): m for m in motion if isinstance(m, dict)}

    field = ACTION_FIELD
    legacy = False
    if shots and ACTION_FIELD not in shots[0] and LEGACY_ACTION_FIELD in shots[0]:
        field, legacy = LEGACY_ACTION_FIELD, True

    report: dict = {
        "field": field,
        "legacy_field": legacy,
        "n_shots": len(shots),
        "n_motion": len(by_shot),
        "missing_motion": [], "missing_shots": [],
        "class_a": [], "class_b": [], "class_c": [],
        "joined": 0,
    }

    motion_indices = set(by_shot)
    shot_indices = {s.get("shot_index") for s in shots}
    report["missing_motion"] = sorted(i for i in shot_indices - motion_indices
                                      if i is not None)
    report["missing_shots"] = sorted(i for i in motion_indices - shot_indices
                                     if i is not None)

    token_lens: list[int] = []
    long_clips = 0
    long_single_beat: list[int] = []

    for shot in shots:
        idx = shot.get("shot_index")
        action = shot.get(field, "")
        beats = count_beats(action)
        no_motion = is_explicit_no_motion(action)
        token_lens.append(_token_len(action))

        try:
            duration = float(shot["end"]) - float(shot["start"])
        except (KeyError, TypeError, ValueError):
            duration = 0.0
        if duration >= LONG_CLIP_S:
            long_clips += 1
            if beats <= 1 or no_motion:
                long_single_beat.append(idx)

        m = by_shot.get(idx)
        if m is None:
            continue
        report["joined"] += 1
        level = m.get("motion")
        flow = m.get("mean_flow_mag")

        if level == "high" and (no_motion or beats <= 1):
            report["class_a"].append({
                "shot": idx, "flow": flow, "duration": round(duration, 2),
                "beats": 0 if no_motion else beats,
                "text": str(action)[:110],
            })
        if level == "high" and says_unchanged(shot.get("end_state")):
            report["class_b"].append({
                "shot": idx, "flow": flow,
                "text": str(shot.get("end_state"))[:110],
            })
        if level == "low" and names_a_camera_move(shot.get("camera_move")):
            report["class_c"].append({
                "shot": idx, "flow": flow,
                "text": str(shot.get("camera_move"))[:110],
            })

    for key in ("class_a", "class_b", "class_c"):
        report[key].sort(key=lambda r: (r["flow"] is None, -(r["flow"] or 0)))

    report["stats"] = {
        "median_action_tokens": (round(statistics.median(token_lens), 1)
                                 if token_lens else 0.0),
        "tokenizer": _tokenizer_name(),
        "long_clips": long_clips,
        "long_single_beat": long_single_beat,
        "long_single_beat_share": (round(len(long_single_beat) / long_clips, 3)
                                   if long_clips else None),
    }
    return report


def _shot_list(rows: list, limit: int = 20) -> str:
    nums = [str(r["shot"]) for r in rows]
    if len(nums) > limit:
        return ", ".join(nums[:limit]) + f", ... (+{len(nums) - limit} more)"
    return ", ".join(nums)


def format_report(report: dict, artifact_path: Path, motion_path: Path) -> str:
    # ASCII only in the printed report: this prints to a Windows console under
    # cp1252, where an em dash lands as a literal replacement character and the
    # operator reads a corrupted line. Source prose above keeps its typography.
    out: list[str] = []
    add = out.append
    n, joined = report["n_shots"], report["joined"]
    add(f"[v589] motion cross-check - {artifact_path.name} vs {motion_path.name}")
    add(f"       {n} shots in the artifact, {report['n_motion']} in motion.json, "
        f"{joined} joined")

    if report["missing_motion"] or report["missing_shots"]:
        add("")
        add("  FINDING: the two files do not describe the same clip list.")
        if report["missing_motion"]:
            add(f"    shots with no motion entry : {report['missing_motion']}")
        if report["missing_shots"]:
            add(f"    motion entries with no shot: {report['missing_shots']}")
        add("    Everything below is joined on the overlap only.")

    if report["legacy_field"]:
        add("")
        add("  NOTE: this artifact predates the ACTION rework - it carries "
            "`visual`, not `action`.")
        add("        `visual` was never asked to record motion, so a "
            "single-beat reading here is")
        add("        EXPECTED and is NOT evidence about the current prompt. "
            "Re-decode to judge that.")

    add("")
    a = report["class_a"]
    label = report["field"]
    add(f"  {len(a)} of {joined} clips: Farneback=high but `{label}` reads as a "
        f"single beat or says nothing moved")
    if a:
        add(f"    shots {_shot_list(a)}")
        add("    worst by optical flow:")
        for row in a[:8]:
            add(f"      shot {row['shot']:>3}  flow={row['flow']:<8} "
                f"{row['duration']}s  beats={row['beats']}  {row['text']!r}")

    b, c = report["class_b"], report["class_c"]
    add("")
    add(f"  secondary - {len(b)} clips: Farneback=high but `end_state` says "
        f"unchanged" + (f" - shots {_shot_list(b)}" if b else ""))
    add(f"  secondary - {len(c)} clips: `camera_move` names a move but "
        f"Farneback=low" + (f" - shots {_shot_list(c)}" if c else ""))

    st = report["stats"]
    add("")
    add("  statistics (reported, not judged):")
    add(f"    median `{label}` length      : {st['median_action_tokens']} tokens "
        f"({st['tokenizer']})")
    add(f"      for scale, the pre-rework `visual` field measured "
        f"{REFERENCE_VISUAL_TOKENS_PER_SHOT} tokens/shot on this same ad in the")
    add(f"      same terse register. A healthy `action` sits ABOVE that - it "
        f"carries several beats where")
    add(f"      `visual` carried one description. A median near or below it, "
        f"with all fields populated,")
    add(f"      is the shape of a collapse to one-verb summaries.")
    share = st["long_single_beat_share"]
    add(f"    clips >= {LONG_CLIP_S}s              : {st['long_clips']}")
    add(f"      of those, single-beat    : {len(st['long_single_beat'])}"
        + (f"  ({share:.0%})" if share is not None else "  (n/a)"))
    if st["long_single_beat"]:
        add(f"      shots {st['long_single_beat'][:20]}")
    add("      A few are normal - a long clip can honestly hold one beat. A "
        "HIGH share is the tell.")
    add("")
    add("  This report never fails a build. Open the shots above and judge them.")
    return "\n".join(out)


def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(
        description="v589 offline motion cross-check: optical flow vs the "
                    "recorded per-clip action (lean adread.v1 lane only)")
    p.add_argument("artifact", type=Path, help="path to a lean stage4d_vlm.json")
    p.add_argument("--motion", type=Path, default=None,
                   help="motion.json (default: beside the artifact)")
    args = p.parse_args(argv)

    if not args.artifact.exists():
        print(f"[v589] error: artifact not found: {args.artifact}",
              file=sys.stderr)
        return 2
    try:
        artifact = load_artifact(args.artifact)
    except NotALeanArtifact as exc:
        print(f"[v589] skipped: {exc}")
        return 0
    except ValueError as exc:
        print(f"[v589] error: could not read {args.artifact}: {exc}",
              file=sys.stderr)
        return 2

    motion_path = args.motion or args.artifact.parent / "motion.json"
    if not motion_path.exists():
        print(f"[v589] skipped: no motion.json at {motion_path}. This report "
              f"needs the v585 Farneback classifications as its second, "
              f"independent source; there is nothing to cross-check against "
              f"and nothing worth inventing.")
        return 0
    try:
        motion = json.loads(motion_path.read_text(encoding="utf-8-sig"))
    except ValueError as exc:
        print(f"[v589] error: could not read {motion_path}: {exc}",
              file=sys.stderr)
        return 2
    if not isinstance(motion, list):
        print(f"[v589] skipped: {motion_path} is not a list of per-shot "
              f"classifications.")
        return 0

    print(format_report(cross_check(artifact, motion), args.artifact,
                        motion_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
