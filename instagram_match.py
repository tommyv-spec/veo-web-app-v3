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
# Only clips that made the FINAL CUT count — the export is "all approved clips".
# ============================================================================

FINAL_CUT_APPROVAL = "approved"


def spoken_line(clip):
    """The words actually heard in the render downloaded for this clip.

    `clip` is a plain dict (kept DB-free so these rules stay unit-testable).
    """
    if ((clip.get("rendered_prompt_variant") or "A").upper() == "B"):
        reworded = (clip.get("dialogue_text_b") or "").strip()
        if reworded:
            return reworded
    return ((clip.get("voiceover_line") or clip.get("dialogue_text")) or "").strip()


def reconstruct_dialogue(clips, final_cut_only=True):
    """Concatenate a job's SPOKEN words, in clip_index order.

    clips: list of dicts with keys id, clip_index, clip_role, paired_clip_id,
           dialogue_text, dialogue_text_b, rendered_prompt_variant,
           voiceover_line, approval_status.
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
        kept = [
            c for c in clips
            if (c.get("clip_role") or "") == "audio_pair"  # lookup source, never filtered
            or (c.get("approval_status") or "") == FINAL_CUT_APPROVAL
        ]
        text = _emit(kept)
        if text:
            return text
        # Fall through: a job with nothing marked approved (legacy rows) must
        # not reconstruct to BLANK — that would drop it from the candidate pool
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
