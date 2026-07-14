"""Transcript-similarity match between an IG video and candidate Jobs.

Pure-Python — stdlib only (difflib + math + re).
"""
import difflib
import math
import re
from collections import Counter
from datetime import timedelta

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")


def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


# ============================================================================
# v852 — SPOKEN-TEXT RECONSTRUCTION.
#
# The matcher must compare the reel's transcript against the words that were
# actually SAID in the render we downloaded — not against the script we first
# wrote. Two things make those diverge:
#
#   1. Prompt B (v805/v821 policy fallback). When the primary prompt trips a
#      generation-policy block, the worker re-renders with Prompt B, which
#      speaks a REWORDED line. `rendered_prompt_variant` says which prompt
#      produced the downloaded render; `dialogue_text_b` holds the reworded
#      words. Reading dialogue_text alone compares against words nobody spoke.
#
#   2. The b-roll clip pair (v698A). `visual_pair` is a SILENT b-roll visual;
#      the speech is rendered by its `audio_pair` twin, whose dialogue_text
#      duplicates the visual's voiceover_line. So when the AUDIO twin fell back
#      to Prompt B, the visual's voiceover_line is stale — the rebuild has to
#      reach through the pair. (A Prompt-B fallback on the visual twin is
#      irrelevant: that render is silent.)
#
# WHICH CLIPS COUNT — mirror what the export ACTUALLY contains, not what we
# wish it contained. "The exported mp4 = the approved clips" is FALSE:
#
#   * A custom LINEUP export (main.py:9232-9241) selects clips by id where
#     `status == completed` and applies NO approval filter at all — only the
#     `else` branch (main.py:9250-9253) filters on approved. So a lineup job's
#     mp4 legitimately contains pending_review clips.
#   * A post-export REDO flips an approved clip back to pending_review
#     (main.py:12963) while the already-posted mp4 still speaks that line, and
#     terminal redo states (REDO_STUCK / REDO_ZOMBIE / max_attempts) leave it
#     non-approved forever.
#
# Dropping such a clip DELETES a whole line's rare terms from the job's text
# while the reel's transcript still contains them — that costs the TF-IDF
# cosine far more than keeping one slightly stale line. A false EXCLUSION is
# much worse than a false INCLUSION. So the rule is:
#
#   1. lineup present -> the final cut is exactly those clip ids.
#   2. else           -> status == completed AND approval != rejected.
#      (failed / skipped / not-yet-generated clips are not `completed`, so the
#      original pollution targets still go; a pending_review clip is KEPT.)
#   3. audio_pair rows are never filtered — they are the lookup source for
#      their visual twin's spoken words.
#   4. empty selection -> fall back to ALL clips. Never blank a job.
# ============================================================================

FINAL_CUT_APPROVAL = "approved"
EXCLUDED_APPROVAL = "rejected"
RENDERED_STATUS = "completed"


def spoken_line(clip):
    """The words actually heard in the render downloaded for this clip.

    `clip` is a plain dict (kept DB-free so these rules stay unit-testable).
    """
    if ((clip.get("rendered_prompt_variant") or "A").upper() == "B"):
        reworded = (clip.get("dialogue_text_b") or "").strip()
        if reworded:
            return reworded
    return ((clip.get("voiceover_line") or clip.get("dialogue_text")) or "").strip()


def reconstruct_dialogue(clips, final_cut_only=True, lineup_ids=None):
    """Concatenate a job's SPOKEN words, in clip_index order.

    clips: list of dicts with keys id, clip_index, clip_role, paired_clip_id,
           dialogue_text, dialogue_text_b, rendered_prompt_variant,
           voiceover_line, approval_status, status.
    lineup_ids: the job's clip_order_json (list/set of clip ids) or None. When
           present it IS the final cut — the lineup export ignores approval.
    """
    # The audio twin owns the speech for its visual partner.
    audio_by_visual = {}
    for c in clips:
        if (c.get("clip_role") or "") == "audio_pair" and c.get("paired_clip_id"):
            audio_by_visual[c["paired_clip_id"]] = spoken_line(c)

    def _emit(pool):
        parts = []
        for c in sorted(pool, key=lambda x: (x.get("clip_index") or 0)):
            role = c.get("clip_role") or "single"
            if role == "audio_pair":
                continue  # emitted via its visual twin; counting both double-counts
            if role == "visual_pair":
                text = (
                    audio_by_visual.get(c.get("id"))
                    or (c.get("voiceover_line") or "").strip()
                    or (c.get("dialogue_text") or "").strip()
                )
            else:
                text = spoken_line(c)
            if text:
                parts.append(text)
        return " ".join(parts).strip()

    if final_cut_only:
        if lineup_ids:
            wanted = set(lineup_ids)
            def _in_cut(c):
                return c.get("id") in wanted
        else:
            def _in_cut(c):
                return (
                    (c.get("status") or "") == RENDERED_STATUS
                    and (c.get("approval_status") or "") != EXCLUDED_APPROVAL
                )
        kept = [
            c for c in clips
            if (c.get("clip_role") or "") == "audio_pair"  # lookup source, never filtered
            or _in_cut(c)
        ]
        text = _emit(kept)
        if text:
            return text
        # Fall through: a job whose selection came out EMPTY (legacy rows with
        # no status, a stale lineup naming ids that no longer exist) must not
        # reconstruct to BLANK — that would drop it from the candidate pool
        # entirely, which is strictly worse than matching on slightly noisy text.
    return _emit(clips)


def _phrase_boost(a: str, b: str, n: int = 3, per_hit: float = 0.05, cap: float = 0.2) -> float:
    """Bonus when N-grams from `a` appear verbatim in `b`. Caps at `cap`."""
    if not a or not b:
        return 0.0
    a_tokens = a.split()
    b_blob = b
    hits = 0
    for i in range(len(a_tokens) - n + 1):
        gram = " ".join(a_tokens[i:i + n])
        if gram and gram in b_blob:
            hits += 1
    return min(cap, hits * per_hit)


def score(ig_transcript: str, job_dialogue: str) -> float:
    """Similarity in [0, 1] between IG transcript and Job dialogue."""
    a = _normalize(ig_transcript)
    b = _normalize(job_dialogue)
    if not a or not b:
        return 0.0
    # autojunk=False is REQUIRED: difflib's autojunk heuristic treats any
    # element appearing in >1% of a sequence >=200 chars as "junk" and skips
    # it. On char-level strings that junks common letters (e/t/a/o/space),
    # which only happens on LONG strings — i.e. b-roll voiceover scripts.
    # Result: correct b-roll matches lost ~13 pts (0.96 -> 0.83) and capped
    # near ~0.67, dropping below the 0.70 floor. Short on-camera-line jobs
    # (<200 chars) never tripped it, so the bug looked "b-roll only".
    base = difflib.SequenceMatcher(None, a, b, autojunk=False).ratio()
    boost = _phrase_boost(a, b)
    return min(1.0, base + boost)


def best_matches(ig_video, candidate_jobs, full_dialogue, k: int = 5, min_score: float = 0.7):
    """Top-K candidate Jobs above min_score.

    Args:
        ig_video: object with .transcription attribute (str).
        candidate_jobs: iterable of Job objects.
        full_dialogue: callable(job) -> concatenated dialogue str.
        k: max results.
        min_score: filter below this similarity.

    Returns:
        list of {"job_id": str, "score": float}, sorted desc.
    """
    transcript = getattr(ig_video, "transcription", "") or ""
    out = []
    for j in candidate_jobs:
        s = score(transcript, full_dialogue(j))
        if s >= min_score:
            out.append({"job_id": j.id, "score": round(s, 4)})
    out.sort(key=lambda x: x["score"], reverse=True)
    return out[:k]


# ============================================================================
# v822.4 — TF-IDF cosine + margin gate (LOCAL matcher only).
#
# WHY: the char-level `score()` above cannot separate near-duplicate scripts.
# The Korella/Nuri builds share the ED language bank verbatim (same body —
# "your soldier / blood flow / comment saffron / purity line"), swapping only
# the hook + recipe.  Measured offline: 78/100 builds score >=0.70 against the
# WRONG build on `score()`, many at 1.000 — so the auto-matcher confidently
# published the wrong twin.  TF-IDF down-weights the shared boilerplate (every
# build has it -> high document-frequency -> low weight) and rewards the
# distinctive hook/recipe words, and the MARGIN GATE refuses to auto-match
# when the top two candidates are within noise (near-duplicates) -> those go
# to manual pick instead of a wrong auto-publish.
# ============================================================================

def _tokens(s: str):
    return _normalize(s).split()


def rank_tfidf(transcript: str, candidates, idf_power: float = 1.0):
    """Rank candidates by TF-IDF cosine similarity to the transcript.

    Args:
        transcript: the whisper transcript (query).
        candidates: iterable of (job_id, dialogue_text).  IDF is fit on THIS
            pool, so the shared boilerplate is down-weighted relative to each
            script's distinctive words.
        idf_power: exponent on IDF (v822.6). >1 emphasises RARE terms harder,
            which suppresses the "generic attractor" — one long, boilerplate-
            heavy job that otherwise tops many unrelated videos because it
            overlaps everyone's common vocabulary.

    Returns:
        list of {"job_id", "score"} sorted desc; score = cosine in [0, 1].
    """
    q_tokens = _tokens(transcript)
    cand = [(jid, _tokens(d or "")) for jid, d in candidates]
    n_docs = len(cand)
    if not q_tokens or n_docs == 0:
        return []

    df = Counter()
    for _jid, tks in cand:
        for w in set(tks):
            df[w] += 1

    def _idf(w):
        base = math.log((n_docs + 1) / (df.get(w, 0) + 1)) + 1.0
        return base ** idf_power if idf_power != 1.0 else base

    def _vec(tks):
        if not tks:
            return {}
        tf = Counter(tks)
        length = len(tks)
        v = {w: (c / length) * _idf(w) for w, c in tf.items()}
        norm = math.sqrt(sum(x * x for x in v.values())) or 1.0
        return {w: x / norm for w, x in v.items()}

    qv = _vec(q_tokens)
    out = []
    for jid, tks in cand:
        cv = _vec(tks)
        # dot over the smaller vector's keys.
        small, big = (qv, cv) if len(qv) <= len(cv) else (cv, qv)
        s = sum(x * big.get(w, 0.0) for w, x in small.items())
        out.append({"job_id": jid, "score": round(min(1.0, max(0.0, s)), 4)})
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def rank_bm25(transcript: str, candidates, k1: float = 1.5, b: float = 0.75):
    """Rank candidates by BM25 (v822.6 candidate metric).

    BM25 is asymmetric (query-driven), length-normalised (b), and saturating
    (k1) — so a long, generic job cannot dominate just by being long. Raw BM25
    is unbounded; we min-max normalise per query into [0, 1] so the same
    margin gate applies (top becomes 1.0; the margin is how far the runner-up
    trails the winner).
    """
    q_tokens = _tokens(transcript)
    cand = [(jid, _tokens(d or "")) for jid, d in candidates]
    n_docs = len(cand)
    if not q_tokens or n_docs == 0:
        return []
    q_set = set(q_tokens)

    df = Counter()
    lengths = []
    for _jid, tks in cand:
        lengths.append(len(tks))
        for w in set(tks):
            df[w] += 1
    avgdl = (sum(lengths) / n_docs) or 1.0

    def _idf(w):
        d = df.get(w, 0)
        return math.log(1 + (n_docs - d + 0.5) / (d + 0.5))

    raw = []
    for (jid, tks), L in zip(cand, lengths):
        tf = Counter(tks)
        s = 0.0
        for w in q_set:
            f = tf.get(w, 0)
            if not f:
                continue
            s += _idf(w) * (f * (k1 + 1)) / (f + k1 * (1 - b + b * L / avgdl))
        raw.append((jid, s))
    hi = max((s for _j, s in raw), default=0.0) or 1.0
    out = [{"job_id": jid, "score": round(max(0.0, s / hi), 4)} for jid, s in raw]
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def auto_pick(ranked, high: float, margin: float):
    """Return the job_id to AUTO-match, or None when it is ambiguous/low.

    Auto-match only when the top candidate is both confident (>= high) AND
    clearly ahead of the runner-up (top - second >= margin).  A near-duplicate
    twin sits right behind the winner -> small margin -> None -> manual pick.

    Delegates to match_verdict so the auto-publish gate and the reported
    verdict can never drift apart: we auto-match exactly when we would tell a
    human the match is 'confident'.
    """
    if not ranked:
        return None
    if match_verdict(ranked, high, margin)["verdict"] == "confident":
        return ranked[0]["job_id"]
    return None


# ============================================================================
# v852 — HARD TIME CONSTRAINT.
#
# A reel was rendered and exported BEFORE it was posted, so a job created AFTER
# the reel went live cannot possibly be its source. This is a hard fact, and it
# separates near-duplicate twins (same script, built days apart) that the WORDS
# alone cannot tell apart.
#
# We gate on Job.created_at, NOT Job.export_at: export_at was backfilled as
# COALESCE(completed_at, NOW()) (models.py:1168), so on legacy rows it can be a
# MIGRATION timestamp — later than reels posted long before — and filtering on
# it would drop the correct old job. created_at is written at row insert and
# never backfilled.
# ============================================================================

JOB_CREATED_SLACK_DAYS = 1.0


def job_predates_post(job_created_at, posted_at, slack_days=JOB_CREATED_SLACK_DAYS):
    """False only when the job was created AFTER the reel was posted (+slack).

    Unknown timestamps never exclude — an absent posted_at must not silently
    empty the candidate pool.
    """
    if job_created_at is None or posted_at is None:
        return True
    return job_created_at <= posted_at + timedelta(days=slack_days)


# ============================================================================
# v855 — EVIDENCE PICK. The media decides; the words only rank.
#
# Text is powerless here. ~787 jobs, many sharing a script VERBATIM: on the 14
# disputed reels the top-1 and top-2 TF-IDF scores came out EXACTLY equal. A
# ranking that cannot separate two candidates cannot be allowed to publish one.
#
# Two MEDIA signals can, and both were measured on production data:
#
#   DURATION — Job.export_duration_s (ffprobe of the shipped mp4) vs
#   InstagramVideo.duration_s (HikerAPI video_duration). The TRUE match lands
#   within 0.02-0.36s; wrong candidates are SECONDS away.
#
#   WAVEFORM — the loudness envelope of the export vs the posted reel. Duration
#   cannot separate two builds that share a clip structure (their exports come
#   out the same length to the last bit — measured: two reels BOTH at
#   46.02000045776367s). Only the waveform separates the same words at the same
#   length, because it is a different PERFORMANCE.
#
# Thresholds below are MEASURED, not invented. On 23 ground-truth reels the
# waveform claimed decisive 13 times and agreed with duration 13/13 — zero
# contradictions. The separations are bimodal (0.004-0.03 for genuine twins,
# 0.29-0.48 for obvious non-matches), so a 0.05 gate sits in an empty gap.
#
# ABSTAINING IS CORRECT. A true match's similarity can be as low as 0.53 (a
# heavily re-encoded reel), so a LOW score means "I cannot tell", never "it is
# the other one". No evidence -> no automatic decision -> a human picks.
# ============================================================================

DUR_TOLERANCE_S = 1.0      # |reel - export| for the winner
DUR_SEPARATION_S = 0.5     # runner-up must be at least this much further
WAVE_MIN_SIM = 0.90
WAVE_SEPARATION = 0.05

# COST GATE. envelope_similarity is O(lags x frames) in pure Python: a 60s clip
# is ~2400 frames x 81 lags ~= 200k float ops PER PAIR. One reel against 200
# windowed candidates is ~40M ops on the web worker -> gunicorn worker timeout
# -> 502 -> SIGABRT -> killed in-flight DB connections (the 2026-07-06 outage
# shape, which the diag endpoint already produced once). So we compare only
# candidates whose export length is within 1.5s of the reel — anything further
# away is a different render BY DEFINITION and cannot be the source — and hard-
# cap the count, nearest-duration first.
WAVE_DUR_PREFILTER_S = 1.5
WAVE_MAX_COMPARISONS = 12

# A job can only be the source of a reel posted AFTER it was built, and the
# operator posts what they just made: the measured maximum job age at post time
# is 20.99 days. 30 days is that with headroom.
RECENCY_WINDOW_DAYS = 30


def within_recency_window(job_created_at, posted_at, days=RECENCY_WINDOW_DAYS):
    """True when the job was built inside the window that could have produced
    this post: not AFTER the post (job_predates_post) and not absurdly before it.

    Unknown timestamps never exclude — an absent posted_at must not silently
    empty the candidate pool.
    """
    if not job_predates_post(job_created_at, posted_at):
        return False
    if job_created_at is None or posted_at is None:
        return True
    return job_created_at >= posted_at - timedelta(days=days)


def _as_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _duration_pick(reel_duration_s, candidates):
    """(job_id, delta) of the export closest in LENGTH to the reel, or (None, ...).

    Decisive only when the winner is within DUR_TOLERANCE_S *and* the runner-up
    is at least DUR_SEPARATION_S further away. Two candidates equally close =
    too close to call = no pick.
    """
    reel = _as_float(reel_duration_s)
    if reel is None:
        return (None, None)
    scored = []
    for c in candidates:
        d = _as_float(c.get("export_duration_s"))
        if d is None:
            continue
        scored.append((abs(reel - d), c.get("job_id")))
    if not scored:
        return (None, None)
    scored.sort(key=lambda t: t[0])
    best_delta, best_id = scored[0]
    if best_delta > DUR_TOLERANCE_S:
        return (None, None)
    if len(scored) > 1 and (scored[1][0] - best_delta) < DUR_SEPARATION_S:
        return (None, None)   # a twin sits just as close — the length cannot tell them apart
    return (best_id, best_delta)


def _waveform_pick(reel_duration_s, reel_fp, candidates):
    """(job_id, similarity) of the export whose PERFORMANCE matches the reel.

    Returns (None, best_sim_or_None) when nothing is decisive — the caller must
    treat that as ABSTAIN, never as evidence against the top candidate.
    """
    if not reel_fp:
        return (None, None, {})
    from audio_fingerprint import decode_fingerprint, envelope_similarity
    reel_env = decode_fingerprint(reel_fp)
    if not reel_env:
        return (None, None, {})

    reel = _as_float(reel_duration_s)
    if reel is None:
        return (None, None, {})   # without a reel length the cost gate cannot be applied
    pool = []
    for c in candidates:
        if not c.get("export_audio_fp"):
            continue
        d = _as_float(c.get("export_duration_s"))
        if d is None:
            # An unverifiable length cannot pass the cost gate, and in practice a
            # job with a fingerprint always has one (the fp backfill only ran on
            # jobs whose duration was already probed).
            continue
        delta = abs(reel - d)
        if delta > WAVE_DUR_PREFILTER_S:
            continue   # cannot be the same render; skip the expensive compare
        pool.append((delta, c))
    pool.sort(key=lambda t: t[0])

    sims = {}
    ranked = []
    for _delta, c in pool[:WAVE_MAX_COMPARISONS]:
        env = decode_fingerprint(c.get("export_audio_fp"))
        if not env:
            continue
        s = envelope_similarity(reel_env, env)
        sims[c.get("job_id")] = round(s, 4)
        ranked.append((s, c.get("job_id")))
    if not ranked:
        return (None, None, sims)
    ranked.sort(key=lambda t: -t[0])
    best_sim, best_id = ranked[0]
    if best_sim < WAVE_MIN_SIM:
        return (None, round(best_sim, 4), sims)   # ABSTAIN, not "the other one"
    if len(ranked) > 1 and (best_sim - ranked[1][0]) < WAVE_SEPARATION:
        return (None, round(best_sim, 4), sims)
    return (best_id, round(best_sim, 4), sims)


def evidence_pick(reel_duration_s, reel_fp, candidates):
    """Decide WHICH job produced this video, from media evidence alone.

    candidates: list of dicts {job_id, export_duration_s, export_audio_fp}.

    Returns {"job_id": <id|None>, "source": "waveform"|"duration"|"waveform+duration"|None,
             "similarity": float|None, "dur_delta": float|None, "conflict": bool}

    Missing data (no reel duration, no fp, no candidates) -> job_id None. NEVER
    raises, NEVER guesses.
    """
    out = {"job_id": None, "source": None, "similarity": None,
           "dur_delta": None, "conflict": False}
    if not candidates:
        return out
    try:
        dur_id, dur_delta = _duration_pick(reel_duration_s, candidates)
        wave_id, wave_sim, sims = _waveform_pick(reel_duration_s, reel_fp, candidates)
    except Exception as e:   # a malformed fp must never break a transcription
        print(f"[evidence] pick failed: {type(e).__name__}: {str(e)[:120]}", flush=True)
        return out

    if dur_id and wave_id and dur_id != wave_id:
        # The two signals contradict each other. Never seen in validation — and a
        # disagreement means we do not understand the data, so guessing here is
        # exactly the failure this function exists to fix. Hand it to a human.
        out["conflict"] = True
        out["similarity"] = wave_sim
        out["dur_delta"] = dur_delta
        return out

    def _delta_of(jid):
        reel = _as_float(reel_duration_s)
        for c in candidates:
            if c.get("job_id") == jid:
                d = _as_float(c.get("export_duration_s"))
                if reel is None or d is None:
                    return None
                return round(abs(reel - d), 4)
        return None

    if wave_id:
        out["job_id"] = wave_id
        out["source"] = "waveform+duration" if dur_id == wave_id else "waveform"
        out["similarity"] = wave_sim
        out["dur_delta"] = _delta_of(wave_id)
    elif dur_id:
        out["job_id"] = dur_id
        out["source"] = "duration"
        out["dur_delta"] = round(dur_delta, 4) if dur_delta is not None else None
        out["similarity"] = sims.get(dur_id)   # reported when computed; never gating
    else:
        out["similarity"] = wave_sim           # for the log line: "how close did we get"
        out["dur_delta"] = None
    return out


def match_verdict(ranked, high, margin):
    """Classify a ranking so the UI can refuse to present a guess as a fact.

    confident — top clears `high` AND clearly beats the runner-up.
    ambiguous — top is strong but a twin sits within `margin`: the words cannot
                tell them apart, so a human must.
    weak      — nothing scored well enough to trust.
    """
    if not ranked:
        return {"verdict": "none", "top": 0.0, "gap": 0.0}
    top = ranked[0]["score"]
    second = ranked[1]["score"] if len(ranked) > 1 else 0.0
    # Compare the UNROUNDED gap: rounding first turns a hair-thin 0.11996 into
    # 0.12, which clears a 0.12 margin and reports a coin-flip between twins as
    # a certainty — exactly what this gate exists to prevent. Round only for
    # what we hand back to be displayed.
    gap = top - second
    if top < high:
        verdict = "weak"
    elif gap < margin:
        verdict = "ambiguous"
    else:
        verdict = "confident"
    return {"verdict": verdict, "top": top, "gap": round(gap, 4)}


# ============================================================================
# v856 — THE FILENAME IS THE ANSWER.
#
# Everything above this line is a GUESS. Text, duration, waveform: all of them
# are measurements of the file, compared against measurements of a job, with a
# threshold in between. They exist because the watcher was handed an mp4 and
# asked "which job made this?" with nothing but the bytes to go on.
#
# But the platform MINTED that mp4, and it named it. So it can simply write the
# answer down. `final_export_<job8>_<timestamp>_<hash>.mp4` carries the job id
# in the name; reading it back is a LOOKUP, and a lookup cannot be wrong.
#
# Two rules keep it that way:
#
#   1. The stamp counts ONLY in the slot the minter puts it in — right after
#      the prefix, and followed by the export DATE. This is not pedantry:
#
#         legacy   final_export_<YYYYMMDD>_<HHMMSS>_<hash6>.mp4
#         stamped  final_export_<job8>_<YYYYMMDD>_<HHMMSS>_<hash6>.mp4
#
#      A date like `20260714` is EIGHT VALID HEX CHARACTERS sitting in exactly
#      the slot the job id now occupies. A pattern that asks only for
#      `[0-9a-f]{8}` after the prefix therefore reads EVERY legacy export ever
#      shipped as job id "20260714" — which finds no job at best, and the wrong
#      job the day a real id starts with those digits. What actually separates
#      the two shapes is what comes NEXT: 8 digits (a date) in the stamped name,
#      only 6 (a time) in the legacy one. So the date is part of the pattern.
#
#   2. A name with no stamp (renamed by the operator, or minted before this
#      rule existed) returns None and the caller falls back to the evidence
#      path. We never DOWNGRADE a guess into a certainty.
# ============================================================================

# final_export_<job8>_<YYYYMMDD>...  — the trailing date is load-bearing, see rule 1.
# It is matched but not captured; only the id is handed back.
_JOB_ID_IN_FILENAME = re.compile(
    r"final_(?:export|broll)_([0-9a-f]{8})_\d{8}", re.IGNORECASE
)


def export_job_segment(job_id):
    """The 8 chars of `job_id` that go INTO an export filename, or None.

    The minting half of the contract (main.py calls this; job_id_from_filename
    reads it back). Both halves live here so they cannot drift apart — the day
    they disagree, every watcher silently stops matching.

    Job ids are uuid4 hex. 8 hex chars = 4.3 billion values, plenty to pin a
    single operator's jobs, and short enough to keep the name readable. An id
    that is NOT hex cannot be stamped without breaking the reader, so it is not
    stamped at all: the caller keeps the legacy name shape and the watcher
    keeps using evidence. Degrade, never corrupt.
    """
    if not isinstance(job_id, str):
        return None
    seg = job_id[:8].lower()
    if len(seg) != 8 or not all(c in "0123456789abcdef" for c in seg):
        return None
    return seg


def job_id_from_filename(file_name):
    """The job id the platform stamped into its own export filename, or None.

    This is a LOOKUP, not a guess: the platform minted this name, so when it is
    present it beats every heuristic — text, duration, waveform. Returns the
    8-char prefix; the caller resolves it to a full job id.

    Lowercased on the way out: job ids are lowercase hex and callers feed this
    into a case-sensitive SQL LIKE.
    """
    if not isinstance(file_name, str) or not file_name:
        return None
    m = _JOB_ID_IN_FILENAME.search(file_name)
    return m.group(1).lower() if m else None
