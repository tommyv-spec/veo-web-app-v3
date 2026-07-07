"""Transcript-similarity match between an IG video and candidate Jobs.

Pure-Python — stdlib only (difflib + math + re).
"""
import difflib
import math
import re
from collections import Counter

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
    """
    if not ranked:
        return None
    top = ranked[0]["score"]
    second = ranked[1]["score"] if len(ranked) > 1 else 0.0
    if top >= high and (top - second) >= margin:
        return ranked[0]["job_id"]
    return None
