"""Transcript-similarity match between an IG video and candidate Jobs.

Pure-Python — stdlib only (difflib + re).
"""
import difflib
import re

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")


def _normalize(s: str) -> str:
    s = (s or "").lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


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
