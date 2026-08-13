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

# Sequence and list separators. "as" and "while" mark simultaneity rather than
# a new beat and are left out. Any comma is enough to save a value from being
# called single-beat, so the bias stays toward UNDER-flagging.
#
# Bare "and" was a separator here until it was checked against the real corpus,
# where EVERY value it split turned out to be a false positive: "hands stitch
# fabric with yellow needle and thread", "adjust a completed red and lace
# decorative bow", "the elderly woman and girl sitting together" — noun
# conjunctions, not beats — plus compound verbs describing one continuous
# movement ("lift and adjust", "open and flip", "smiles and nods"). Not one
# genuine sequential pair among them. It is gone.
_BEAT_SPLIT = re.compile(r"\bthen\b|;|,", re.IGNORECASE)

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

# ── The two duration thresholds, which answer DIFFERENT questions ─────
#
# MIN_BEAT_SECONDS is a FLAG FLOOR: "could this clip physically have held two
# beats?" Below it, "you only recorded one beat" is not an accusation, it is a
# description of the clip, and flagging it is noise. The real 80-clip ad had a
# 0.3s clip carrying the highest optical flow in the whole ad; there is no such
# thing as a multi-beat 0.3s clip.
#
# HONEST PROVENANCE: this is a PHYSICAL floor, not a corpus-derived one, because
# the corpus could not supply one. Checking every clip in both reads of the real
# ad that scored >=2 beats found ZERO genuine sequential pairs at ANY duration —
# they were all noun conjunctions or compound verbs (see _BEAT_SPLIT). With no
# multi-beat records anywhere in the data there is no empirical cliff to read a
# floor off, and dressing one up as data-derived would be a fabricated number.
#
# So it rests on gesture timing instead: one discrete manual beat (reach, grasp,
# turn, release) runs ~0.3-0.5s, so showing two needs ~1s at minimum. The floor
# only ever SUPPRESSES flags, so erring high costs recall and never precision.
# Override with --min-beat-seconds; the report prints the value used and how many
# clips it excluded, because a silent filter is what this report exists to
# prevent.
MIN_BEAT_SECONDS = 1.0

# ── What "high motion" means, per ad ──────────────────────────────────
#
# The primary class used v585's CATEGORICAL motion label. That label cuts at
# mean_flow_mag > 0.7, and on the real 80-clip ad the MEDIAN flow is 1.539 —
# more than double the cut — so "high" was true of 60 of 80 clips. A predicate
# true of three quarters of the corpus by construction cannot discriminate, and
# no duration floor repairs that.
#
# So the primary class reads the CONTINUOUS mean_flow_mag against the ad's OWN
# distribution: high means high FOR THIS AD.
#
# 2x the median rather than p90, deliberately. A percentile is a fixed RATE: p90
# flags the top 10% of every ad by construction, so a flawless artifact still
# reports ~8 flags on 80 clips and the number says nothing. A multiple of the
# median is a SHAPE measure — an ad whose clips all move about the same amount
# produces few or ZERO clips above 2x median, which is what a clean result has
# to be able to look like. Both are scale-invariant; only one can come back
# empty. The report prints the cut value AND the percentile it landed on, so a
# reader sees what "high" meant here instead of trusting the word.
FLOW_MULTIPLE = 2.0

# Below this many joined clips a median is not stable — one outlier moves it —
# so a per-ad distribution would be a confident number off almost no samples.
# A judgement call, not a derivation, and said out loud here for the same reason
# MIN_BEAT_SECONDS is: 12 is where a median stops swinging on a single clip.
# Under it the report falls back to v585's categorical label and SAYS it did.
MIN_CLIPS_FOR_DISTRIBUTION = 12

# NOT A CHANGE TO v585. `motion.json`'s categorical `motion` field is shared —
# the heavy stage4d.v2 lane's per-shot motion_cross_check reads it, and so does
# anything else consuming that file. This module changes only how THIS REPORT
# reads the continuous value sitting next to it. Do not "fix" the inconsistency
# by moving v585's threshold: the categorical label is not wrong, it is calibrated
# for a different question, and re-cutting it would silently change every other
# consumer. Classes B and C still read the categorical on purpose, so their counts
# stay comparable across runs; class D shares the primary's cut because it is the
# primary's sub-floor overflow and the two must agree about the same clip.

# LONG_CLIP_S is the STATISTIC's bar: not "could it hold two beats" but "is it
# long enough that several beats would be unremarkable". Deliberately well above
# the flag floor, and reported next to the ad's median clip duration, because a
# single-beat share means nothing without the ad's cutting rhythm as denominator.
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


def flow_cut_for(motion: list, multiple: float = FLOW_MULTIPLE) -> dict:
    """What counts as high motion FOR THIS AD, read off its own distribution.

    Returns the cut, the median it came from, the percentile the cut landed on,
    and — when the sample is too small for a median to mean anything — a
    `basis` of "categorical" so the caller falls back to v585's label and the
    report can say it did rather than print a confident number off five clips.
    """
    values = sorted(float(m["mean_flow_mag"]) for m in motion
                    if isinstance(m, dict)
                    and isinstance(m.get("mean_flow_mag"), (int, float)))
    if len(values) < MIN_CLIPS_FOR_DISTRIBUTION:
        return {"basis": "categorical", "cut": None, "median": None,
                "percentile": None, "n": len(values), "multiple": multiple}
    median = statistics.median(values)
    if median <= 0:
        # Every clip is a still. A multiple of zero is not a threshold.
        return {"basis": "categorical", "cut": None, "median": median,
                "percentile": None, "n": len(values), "multiple": multiple}
    cut = multiple * median
    at_or_below = sum(1 for v in values if v < cut)
    return {"basis": "distribution", "cut": round(cut, 4),
            "median": round(median, 4),
            "percentile": round(100 * at_or_below / len(values)),
            "n": len(values), "multiple": multiple}


def cross_check(artifact: dict, motion: list,
                min_beat_seconds: float = MIN_BEAT_SECONDS,
                flow_multiple: float = FLOW_MULTIPLE) -> dict:
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
        "class_a": [], "class_b": [], "class_c": [], "class_d": [],
        "joined": 0,
        "min_beat_seconds": min_beat_seconds,
        # Two DIFFERENT numbers, kept apart because conflating them makes the
        # denominator lie: `short_clips` is every joined clip below the floor
        # (the ineligible population), `excluded_short` is only those the floor
        # actually stopped from being flagged (the suppressed flags).
        "short_clips": [],
        "excluded_short": [],
        "flow": flow_cut_for(motion, flow_multiple),
    }
    cut = report["flow"]["cut"]

    def moved_a_lot(level: object, flow: object) -> bool:
        """Used by the PRIMARY class and by class D, which is the primary's
        sub-floor overflow and must share its definition of "moved a lot" or
        the two disagree about the same clip. Classes B and C keep v585's
        categorical label: they are independent questions about end_state and
        camera_move, and holding them fixed keeps their counts comparable
        across runs. Falls back to the label when there is no per-ad cut or the
        entry carries no numeric flow."""
        if cut is None or not isinstance(flow, (int, float)):
            return level == "high"
        return float(flow) >= cut

    motion_indices = set(by_shot)
    shot_indices = {s.get("shot_index") for s in shots}
    report["missing_motion"] = sorted(i for i in shot_indices - motion_indices
                                      if i is not None)
    report["missing_shots"] = sorted(i for i in motion_indices - shot_indices
                                     if i is not None)

    token_lens: list[int] = []
    durations: list[float] = []
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
        durations.append(duration)
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
        if duration < min_beat_seconds:
            report["short_clips"].append(idx)

        thin = no_motion or beats <= 1
        if moved_a_lot(level, flow) and thin:
            if duration >= min_beat_seconds:
                report["class_a"].append({
                    "shot": idx, "flow": flow, "duration": round(duration, 2),
                    "beats": 0 if no_motion else beats,
                    "text": str(action)[:110],
                })
            else:
                # Too short to have held two beats — never a class A flag. But
                # the flow still has to be explained by SOMETHING: if the camera
                # is also declared locked off, the two sources still disagree,
                # and folding that into silence is what the floor must not do.
                report["excluded_short"].append(idx)
                if not names_a_camera_move(shot.get("camera_move")):
                    report["class_d"].append({
                        "shot": idx, "flow": flow,
                        "duration": round(duration, 2),
                        "text": f"camera={str(shot.get('camera_move'))[:40]!r} "
                                f"action={str(action)[:60]!r}",
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

    for key in ("class_a", "class_b", "class_c", "class_d"):
        report[key].sort(key=lambda r: (r["flow"] is None, -(r["flow"] or 0)))
    report["excluded_short"].sort()
    report["short_clips"].sort()

    report["stats"] = {
        "median_action_tokens": (round(statistics.median(token_lens), 1)
                                 if token_lens else 0.0),
        "tokenizer": _tokenizer_name(),
        "median_clip_s": (round(statistics.median(durations), 2)
                          if durations else None),
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
    floor = report["min_beat_seconds"]
    excluded = report["excluded_short"]
    eligible = joined - len(report["short_clips"])
    fl = report["flow"]
    if fl["basis"] == "distribution":
        highdesc = f"flow >= {fl['cut']}"
    else:
        highdesc = "Farneback=high"
    add(f"  {len(a)} of {eligible} eligible clips: {highdesc} but `{label}` "
        f"reads as a single beat or says nothing moved")
    if fl["basis"] == "distribution":
        add(f"    high motion = {fl['multiple']}x THIS AD's median flow "
            f"({fl['median']}) = {fl['cut']}, which is its p{fl['percentile']} "
            f"over {fl['n']} clips.")
        add(f"    Read off the ad's own distribution, not a fixed threshold: "
            f"v585's categorical")
        add(f"    `high` cuts at 0.7 and was true of most of this ad by "
            f"construction. A multiple of")
        add(f"    the median can also come back EMPTY on an ad whose clips all "
            f"move alike - a")
        add(f"    percentile never can. Override with --flow-multiple.")
    else:
        add(f"    high motion = v585's categorical label: only {fl['n']} clips "
            f"carry a numeric flow,")
        add(f"    below the {MIN_CLIPS_FOR_DISTRIBUTION} a per-ad median needs "
            f"to mean anything. Treat the count as indicative.")
    add(f"    eligible = clip is at least {floor}s; "
        f"{len(report['short_clips'])} of {joined} joined clips are shorter.")
    add(f"    The floor suppressed {len(excluded)} flag(s) on clips too short "
        f"to hold two beats"
        + (f": shots {excluded[:20]}" if excluded else "."))
    if a:
        add(f"    shots {_shot_list(a)}")
        add("    worst by optical flow:")
        for row in a[:8]:
            add(f"      shot {row['shot']:>3}  flow={row['flow']:<8} "
                f"{row['duration']}s  beats={row['beats']}  {row['text']!r}")
        add("    LIMIT: optical flow cannot tell ONE long gesture from TWO")
        add("           beats. A long clip holding a single continuous")
        add("           movement lands here and is a correct record. This is a")
        add("           question to open, never a verdict.")

    b, c, d = report["class_b"], report["class_c"], report["class_d"]
    add("")
    add(f"  secondary - {len(b)} clips: Farneback=high but `end_state` says "
        f"unchanged" + (f" - shots {_shot_list(b)}" if b else ""))
    add(f"  secondary - {len(c)} clips: `camera_move` names a move but "
        f"Farneback=low" + (f" - shots {_shot_list(c)}" if c else ""))
    add(f"  secondary - {len(d)} clips under {floor}s: {highdesc}, thin "
        f"action AND `camera_move` says the camera held still"
        + (f" - shots {_shot_list(d)}" if d else ""))
    if d:
        add("    The floor excuses a short clip from holding two beats. It does")
        add("    not excuse fast pixels with a locked-off camera and nothing")
        add("    recorded moving - something moved, and nobody wrote it down.")
        for row in d[:5]:
            add(f"      shot {row['shot']:>3}  flow={row['flow']:<8} "
                f"{row['duration']}s  {row['text']}")

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
    add(f"    median clip duration        : {st['median_clip_s']}s "
        f"(the ad's cutting rhythm - the denominator for the line below)")
    add(f"    clips >= {LONG_CLIP_S}s              : {st['long_clips']}")
    add(f"      of those, single-beat    : {len(st['long_single_beat'])}"
        + (f"  ({share:.0%})" if share is not None else "  (n/a)"))
    if st["long_single_beat"]:
        add(f"      shots {st['long_single_beat'][:20]}")
    add("      A high share is a tell ONLY relative to that rhythm. A fast-cut")
    add("      ad whose clips each hold one gesture will legitimately sit near")
    add("      100% and be completely honest. Compare an ad against ITSELF over")
    add("      time, or against an ad cut at a similar pace - never against a")
    add("      fixed target.")
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
    p.add_argument("--min-beat-seconds", type=float, default=MIN_BEAT_SECONDS,
                   help=f"clips shorter than this are never flagged for being "
                        f"single-beat - they cannot physically hold two "
                        f"(default {MIN_BEAT_SECONDS}s; the report prints the "
                        f"value used and how many clips it excluded)")
    p.add_argument("--flow-multiple", type=float, default=FLOW_MULTIPLE,
                   help=f"what counts as high motion for the PRIMARY class, as "
                        f"a multiple of THIS AD's median mean_flow_mag "
                        f"(default {FLOW_MULTIPLE}). Falls back to v585's "
                        f"categorical label under "
                        f"{MIN_CLIPS_FOR_DISTRIBUTION} clips; the report prints "
                        f"the cut, the median and the percentile it landed on")
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

    report = cross_check(artifact, motion,
                         min_beat_seconds=args.min_beat_seconds,
                         flow_multiple=args.flow_multiple)
    print(format_report(report, args.artifact, motion_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
