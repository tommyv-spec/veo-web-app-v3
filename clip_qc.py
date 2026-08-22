"""v939 — clip variant QC: did the render actually SAY the whole line?

Runs LOCALLY (operator box), never on Render. Downloads each rendered variant
of a clip, listens to it two independent ways, and records whether the spoken
line is complete. It POSTS nothing, approves nothing, redoes nothing.

v886.3 IS THE FRAME AND IT NEVER RELAXES, exactly as in image_qc.py: the
operator makes every pick, this module chooses nothing, and QC can never block
a send. Everything below is a recorded opinion — what the machine WOULD have
discarded, so agreement with what the operator actually kept can be measured
BEFORE anything is wired to a real reject.

That order is not caution for its own sake. image_qc's own backtest (119
nodes / 495 variants) found the first verdict rule rejected the operator's own
chosen variant 44.5% of the time, and every one of those rejections was
factually true and severity-blind. A clip checker fails the same way if it is
allowed to fail a take over small wording drift. So this file ships with a
`--backtest` mode whose single most important output is the FALSE-REJECT RATE
against takes the operator already selected. Nothing gets wired to an
auto-discard until that number is measured and small.


WHY TWO SIGNALS, AND WHY THEY MUST AGREE
========================================
Signal 1 — FORCED ALIGNMENT (torchaudio MMS_FA, the same MMS-300M CTC model
transcript_alignment.py already uses at export time). We hand it the line we
EXPECTED and it finds where each word sits in the audio, with a probability
per word. This is the precise instrument: it says WHICH words are missing and
WHERE, which raw transcription cannot.

  The `<star>` token is the whole trick. torchaudio's own CTC-alignment
  tutorial introduces it for exactly our case — "when the transcript is
  partially missing". `MMS_FA.get_model()` includes it by default (our export
  path deliberately turns it off with `with_star=False`, which is right for
  trimming and wrong for detection). Probed on torchaudio 2.7.1: the MMS_FA
  dictionary has 29 entries, `*` is id 28, and the star column of the emission
  is 0.0 in log space — probability 1 at every frame. So a star absorbs any
  audio it is pointed at for free. We wrap the line in one leading and one
  trailing star, which soaks up the v644 pad, room tone and any extra speech
  Veo invents, while every real word still has to be forced through the actual
  audio. A word that was never spoken therefore cannot hide: the aligner is
  obliged to place it somewhere, so it gets crushed into a couple of frames
  with a near-zero score. That crushed span IS the detection.

  Stars are NOT interleaved between every word. That variant is also valid and
  scores real words slightly higher (breaths get absorbed), but it makes every
  word cheap to skip, which is the opposite of what a missing-word detector
  wants mid-line.

Signal 2 — FREE TRANSCRIPTION (faster-whisper distil-large-v3 int8, again
already in the stack). No expectations, just "what do you hear". Then a
word-level edit distance against the script. This is the standard round-trip
check used to score generated speech everywhere — NVIDIA's Riva TTS evaluation
guide describes the same loop, and it is the objective metric in the
generative-speech literature. It is the blunt instrument: it catches "the
model said something else entirely" and "it stopped halfway", and it is immune
to the one way forced alignment lies (alignment ALWAYS produces a path, so a
confident-looking alignment against the wrong audio is possible).

The edit distance is computed with rapidfuzz, which is already a dependency.
jiwer is the usual library for this and would be fine — it computes WER by
calling rapidfuzz underneath — but pulling a package to wrap a function we can
call directly is a dependency for nothing.

NEITHER SIGNAL MAY HARD-FAIL A VARIANT ALONE. Alignment alone over-fires on
accents and mumbles; transcription alone over-fires on homophones and on Veo's
odd pronunciation of brand names. Requiring both to point at the same missing
words is what keeps the false-reject rate near zero, and near zero is the only
thing that makes an auto-discard safe to switch on later.


THE VERDICT IS STRUCTURAL
=========================
Copied deliberately from image_qc.py §v936.4. A variant FAILS if and only if
one of the short HARD list fires:

    no_speech          nothing intelligible in the clip at all
    tail_truncated     the END of the line is missing, per BOTH signals
    line_missing       coverage collapsed AND the transcript disagrees badly

Everything else the checker notices — a dropped word mid-line, extra speech,
low-confidence words, a last word that ends flush against the end of the file
— is a WARNING: true, recorded, and unable to fail a variant in either
direction. `tail_truncated` is the one this file exists for.


WHAT WOULD BREAK A NAIVE VERSION (all three are live in this repo)
==================================================================
1. v644 `dialogue_pad` — short lines get padded so Veo's audio path does not
   fail, so the clip contains extra spoken words ON PURPOSE. We always score
   against the bare line and let the trailing star eat the pad. Extra speech
   is a note, never a fail.
2. v821 Prompt B — when `rendered_prompt_variant == 'B'` the clip spoke
   `dialogue_text_b`, a REWORDED line. Scoring a B render against line A fails
   every one of them. We score against the declared line and, when both exist,
   evaluate both and keep the better, recording which one matched.
3. v698A pairs — a `visual_pair` clip is silent by design and its line lives
   on the `audio_pair` twin. Checking the silent half would fail every pair.
   Skipped by rule, with the reason recorded rather than dropped in silence.

Cache: every download + alignment + transcription is written to
~/.kaveno/clipqc-cache/ as raw evidence. Scoring is pure arithmetic over that
cache, so a threshold sweep across a whole backtest costs no GPU-seconds and
no network. The expensive pass runs once.

CLI
    python code/clip_qc.py --job <job_id>              score one job
    python code/clip_qc.py --since-days 30 --limit 20  score recent jobs
    python code/clip_qc.py --backtest --since-days 60  agreement + sweep
    python code/clip_qc.py --job <id> --out report.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ============================================================================
# Thresholds. Every one of these is a GUESS until --backtest replaces it with
# a measured number. They are module constants rather than env vars so that a
# calibration run and a scoring run cannot silently disagree; the sweep in
# --backtest prints the grid it explored so a change here is always traceable
# to the run that justified it.
# ============================================================================

# --- alignment side -------------------------------------------------------
WORD_CONF_FLOOR = 0.40   # mean token probability below this = not really said
WORD_MIN_DUR_S = 0.035   # a word crushed into ~1 frame (20ms) was not spoken
COVERAGE_FLOOR = 0.75    # fraction of script words that must land

# --- transcription side ---------------------------------------------------
WER_CEILING = 0.50       # word error rate above this = a different line
TAIL_FUZZ = 80           # rapidfuzz partial_ratio needed to call the tail present

# --- the tail rule, which is what this file is for ------------------------
# 1, not 2. Measured on a real cut (tools/verify_clip_qc_detector.py): trimming
# a 3.85s line to 60% removes exactly ONE word, and a video whose last word is
# gone is broken. A count of 2 let that through. The protection against firing
# on a clipped breath is not this number — it is the third condition in the
# rule, which requires the ending to be missing from the transcript ENTIRELY.
TAIL_FAIL_WORDS = 1      # this many final words missing (both signals) = FAIL
TAIL_ROOM_S = 0.15       # last word ending this close to EOF = hard-cut warning
# Past this many missing final words, the fuzzy veto stops applying. The veto
# exists to forgive ONE misheard final word; it must not be able to forgive a
# whole missing clause. Measured on real Veo audio (docs/experiments/
# clip-qc-2026-08-22.md §5): all three truncations the rule missed at a 55% cut
# had the same cause — the line's final word also occurs earlier in the line
# ("...not on a good NIGHT. every single NIGHT."), so the veto matched its
# earlier self and vetoed a rejection over 3, 5 and 16 missing words.
TAIL_VETO_LIMIT = 3

# --- housekeeping ---------------------------------------------------------
# distil-large-v3 is what transcript_alignment.py already audits with, so this
# introduces no new accuracy question. Measured on this box (8 cores, 3.85s of
# speech): 17.8s at faster-whisper's default thread count, 10.4s with all
# cores, against 1.75s for the alignment pass — the transcription IS the cost
# of a run. distil-small.en does the same 3.85s in 3.6s and got the fixture
# word-perfect, so it is the lever if a big backtest is too slow to sit
# through; it is not the default because a weaker transcript makes the tail
# veto fire less often, and that pushes the false-reject rate the wrong way.
ASR_MODEL_ID = os.environ.get("CLIPQC_ASR_MODEL", "distil-large-v3")
ASR_THREADS = int(os.environ.get("CLIPQC_ASR_THREADS", "0")) or (os.cpu_count() or 4)
CACHE_DIR = Path(os.path.expanduser("~")) / ".kaveno" / "clipqc-cache"
EVIDENCE_VERSION = 1     # bump when the cached evidence shape changes


# ============================================================================
# Text normalisation — pure, no imports beyond re
# ============================================================================

# Whisper writes "45", the script writes "forty-five", and MMS_FA's vocabulary
# is [a-z'] so a digit cannot align at all. Both sides go through the same
# expansion. This covers 0-100 plus the round hundreds/thousands, which is
# every number that has actually appeared in a line in this corpus (ages,
# prices, day counts). A number outside the table survives as digits and will
# read as a mismatch on the transcription side — a KNOWN GAP, recorded here
# rather than papered over, because a silently wrong expansion is worse than a
# visible miss.
_ONES = ["zero", "one", "two", "three", "four", "five", "six", "seven",
         "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
         "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
_TENS = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy",
         "eighty", "ninety"]


def _number_to_words(n: int) -> str:
    """0-9999 as spoken English words. Outside that range, the digits back."""
    if n < 0 or n > 9999:
        return str(n)
    if n < 20:
        return _ONES[n]
    if n < 100:
        tens, ones = divmod(n, 10)
        return _TENS[tens] + (" " + _ONES[ones] if ones else "")
    if n < 1000:
        hundreds, rest = divmod(n, 100)
        out = _ONES[hundreds] + " hundred"
        return out + (" " + _number_to_words(rest) if rest else "")
    thousands, rest = divmod(n, 1000)
    out = _number_to_words(thousands) + " thousand"
    return out + (" " + _number_to_words(rest) if rest else "")


def _expand_numbers(text: str) -> str:
    """Every run of digits replaced by its spoken form. Money and percents
    keep their unit as a word so "$40" and "forty dollars" agree."""
    text = re.sub(r"\$\s*(\d+)", lambda m: _number_to_words(int(m.group(1))) + " dollars", text)
    text = re.sub(r"(\d+)\s*%", lambda m: _number_to_words(int(m.group(1))) + " percent", text)
    return re.sub(r"\d+", lambda m: _number_to_words(int(m.group(0))), text)


def normalize_words(text: str) -> List[str]:
    """Lowercase word list in the MMS_FA alphabet: [a-z'] only.

    Mirrors transcript_alignment._normalize_script_for_mms so the aligner
    cannot be handed a token it will raise a KeyError on, and so both signals
    are comparing the same shape of text. Hyphens and dashes split words;
    every other punctuation mark is dropped.
    """
    if not text:
        return []
    cleaned = _expand_numbers(text.lower())
    cleaned = cleaned.replace("—", " ").replace("–", " ").replace("-", " ")
    cleaned = re.sub(r"[^a-z'\s]", " ", cleaned)
    return [w.strip("'") for w in cleaned.split() if w.strip("'")]


# ============================================================================
# Which line did this clip actually try to say, and should we score it at all
# ============================================================================

SKIP_NOT_COMPLETED = "clip is not completed"
SKIP_NO_LINE = "clip has no dialogue line (text card or silent)"
SKIP_VISUAL_PAIR = "v698A visual_pair: silent by design, its audio twin carries the line"
SKIP_NO_VARIANTS = "no rendered variants on disk"
SKIP_NOT_SPEECH = "dialogue_text is build-file syntax, not a spoken line"


def looks_like_speech(text: str) -> bool:
    """Is this actually a line someone says?

    Found in the wild on the first real run: clip 14522 carried
    `- **clip_duration_s:** 4` in `dialogue_text`, a markdown bullet from the
    build file that the importer had swallowed as the spoken line. The clip is
    silent, so the checker dutifully reported `no_speech` — a true statement
    about a clip that was never supposed to say anything, and a false reject
    as far as the operator is concerned.

    This is a guard, not a fix: the real bug is upstream in whatever wrote that
    row, and it is worth chasing separately. Skipping is right regardless —
    there is no line here to check against.
    """
    stripped = (text or "").strip()
    if not stripped:
        return False
    if stripped.startswith("- ") or stripped.startswith("**"):
        return False
    if "**" in stripped or "_s:**" in stripped:
        return False
    return len(normalize_words(stripped)) >= 2


def expected_lines(clip: Dict[str, Any]) -> List[Tuple[str, str]]:
    """The line(s) this clip may legitimately have spoken, best guess first.

    v821: `rendered_prompt_variant` says which prompt produced the CURRENT
    render, but it is a single column on the clip while variants accumulate
    across attempts — an earlier attempt may have used the other prompt. So
    the declared one leads and the other follows as a fallback, and the caller
    keeps whichever scores better while recording which line matched. That is
    honest about the ambiguity instead of pretending the column is per-variant.
    """
    a = (clip.get("dialogue_text") or "").strip()
    b = (clip.get("dialogue_text_b") or "").strip()
    declared = (clip.get("rendered_prompt_variant") or "A").upper()
    out: List[Tuple[str, str]] = []
    if declared == "B" and b:
        out.append(("B", b))
        if a:
            out.append(("A", a))
    else:
        if a:
            out.append(("A", a))
        if b:
            out.append(("B", b))
    return out


def should_score_clip(clip: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """(score it?, reason not to). Every skip carries a reason so a run that
    scores 3 of 11 clips says why the other 8 were left alone."""
    if (clip.get("clip_role") or "") == "visual_pair":
        return False, SKIP_VISUAL_PAIR
    if (clip.get("status") or "") != "completed":
        return False, SKIP_NOT_COMPLETED
    lines = expected_lines(clip)
    if not lines:
        return False, SKIP_NO_LINE
    if not any(looks_like_speech(text) for _, text in lines):
        return False, SKIP_NOT_SPEECH
    if not variant_files(clip):
        return False, SKIP_NO_VARIANTS
    return True, None


def variant_files(clip: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One entry per rendered take: {attempt, filename}. Deduplicated on
    attempt number, keeping the last, which is what the API's own
    deduplicate_versions does. `output_filename` is folded in because the
    current take is not always present in versions_json."""
    seen: Dict[int, Dict[str, Any]] = {}
    for v in (clip.get("versions") or []):
        if not isinstance(v, dict):
            continue
        fn = v.get("filename")
        attempt = v.get("attempt")
        if fn and isinstance(attempt, int):
            seen[attempt] = {"attempt": attempt, "filename": fn}
    current = clip.get("output_filename")
    if current:
        attempt = clip.get("generation_attempt") or 1
        seen.setdefault(attempt, {"attempt": attempt, "filename": current})
    return [seen[k] for k in sorted(seen)]


# ============================================================================
# Scoring — PURE. Takes cached evidence dicts, returns metrics. No torch, no
# ffmpeg, no network, so the whole threshold sweep runs in milliseconds and
# every branch below is reachable from a unit test.
# ============================================================================

def word_is_spoken(word: Dict[str, Any],
                   conf_floor: float = WORD_CONF_FLOOR,
                   min_dur_s: float = WORD_MIN_DUR_S) -> bool:
    """Did the audio actually contain this script word?

    Two conditions, because either alone is fooled. A low score with a normal
    duration is usually a mumble or an accent, and a good score crushed into
    one frame is the aligner being forced to place a word that is not there.
    Only when BOTH look wrong is the word treated as absent.
    """
    conf = float(word.get("confidence") or 0.0)
    dur = float(word.get("end") or 0.0) - float(word.get("start") or 0.0)
    return conf >= conf_floor and dur >= min_dur_s


def score_alignment(words: Sequence[Dict[str, Any]],
                    audio_duration: float,
                    conf_floor: float = WORD_CONF_FLOOR,
                    min_dur_s: float = WORD_MIN_DUR_S) -> Dict[str, Any]:
    """Alignment metrics for one variant against one line.

    `tail_missing` is the number of words missing counting BACK from the end
    of the line, and it is the headline number: a clip that was cut off has
    its last words missing while everything before them aligned fine, which is
    a completely different shape from a clip that mumbled a word in the middle.
    """
    total = len(words)
    if total == 0:
        return {
            "script_words": 0, "spoken_words": 0, "coverage": 0.0,
            "missing": [], "tail_missing": 0, "head_missing": 0,
            "longest_missing_run": 0, "last_word_end": 0.0,
            "tail_room_s": 0.0, "ends_flush": False, "mean_confidence": 0.0,
        }

    spoken = [word_is_spoken(w, conf_floor, min_dur_s) for w in words]

    tail_missing = 0
    for ok in reversed(spoken):
        if ok:
            break
        tail_missing += 1
    head_missing = 0
    for ok in spoken:
        if ok:
            break
        head_missing += 1

    longest = run = 0
    for ok in spoken:
        run = 0 if ok else run + 1
        longest = max(longest, run)

    said = [w for w, ok in zip(words, spoken) if ok]
    last_end = max((float(w.get("end") or 0.0) for w in said), default=0.0)
    room = max(0.0, float(audio_duration or 0.0) - last_end)

    return {
        "script_words": total,
        "spoken_words": sum(spoken),
        "coverage": sum(spoken) / total,
        "missing": [w.get("text") for w, ok in zip(words, spoken) if not ok],
        "tail_missing": tail_missing,
        "head_missing": head_missing,
        "longest_missing_run": longest,
        "last_word_end": round(last_end, 3),
        "tail_room_s": round(room, 3),
        # A last word that finishes flush against the end of the file is what a
        # hard cut looks like. On its own it is only suspicious — a well-timed
        # render legitimately ends soon after the last word — so it is evidence
        # that strengthens the tail case, never a failure by itself.
        "ends_flush": bool(said) and room <= TAIL_ROOM_S,
        "mean_confidence": round(
            sum(float(w.get("confidence") or 0.0) for w in words) / total, 4),
    }


def _opcodes(ref: Sequence[str], hyp: Sequence[str]):
    """Word-level edit operations, ref -> hyp. rapidfuzz operates on any
    sequence of hashables, so word lists go in directly; this is the same
    computation jiwer performs, without the extra dependency."""
    from rapidfuzz.distance import Levenshtein
    return Levenshtein.opcodes(list(ref), list(hyp))


def score_transcript(ref_words: Sequence[str],
                     hyp_words: Sequence[str]) -> Dict[str, Any]:
    """Transcription metrics: word error rate plus WHICH script words the
    transcript has no counterpart for.

    Deletions and substitutions both count as "not heard". A substitution is
    kept separate in the report because Veo mispronouncing a brand name and
    Veo omitting a clause both show up as substitutions at the word level, and
    only the operator can tell those apart.
    """
    ref = list(ref_words)
    hyp = list(hyp_words)
    if not ref:
        return {"wer": 0.0, "ref_words": 0, "hyp_words": len(hyp),
                "unheard_flags": [], "deleted": [], "substituted": [],
                "inserted": 0, "tail_unheard": 0}

    flags = [False] * len(ref)   # True = this script word has no counterpart
    deleted: List[str] = []
    substituted: List[str] = []
    inserted = 0
    edits = 0
    for tag, i1, i2, j1, j2 in _opcodes(ref, hyp):
        if tag == "equal":
            continue
        if tag == "delete":
            edits += i2 - i1
            for i in range(i1, i2):
                flags[i] = True
                deleted.append(ref[i])
        elif tag == "replace":
            edits += max(i2 - i1, j2 - j1)
            for i in range(i1, i2):
                flags[i] = True
                substituted.append(ref[i])
        elif tag == "insert":
            edits += j2 - j1
            inserted += j2 - j1

    tail_unheard = 0
    for ok in reversed(flags):
        if not ok:
            break
        tail_unheard += 1

    return {
        "wer": round(edits / len(ref), 4),
        "ref_words": len(ref),
        "hyp_words": len(hyp),
        "unheard_flags": flags,
        "deleted": deleted,
        "substituted": substituted,
        "inserted": inserted,
        "tail_unheard": tail_unheard,
    }


def tail_present_in_transcript(ref_words: Sequence[str],
                               hyp_words: Sequence[str],
                               tail_len: int = 3,
                               threshold: int = TAIL_FUZZ) -> bool:
    """Is the END of the line in what was heard?

    Deliberately fuzzy and deliberately independent of the edit path above.
    That path is global: one bad patch in the middle can shove the whole tail
    out of step and make a perfectly-spoken ending look deleted. This check is
    immune to that, so a tail failure has to survive both readings.

    It asks about the FINAL WORD specifically, not the last few words as one
    string. Measured, because the obvious version is wrong: searching for
    "stood back up" inside "his soldier stood back" scores 87 on
    partial_ratio and calls a missing "up" present — the two words that DID
    survive carry the match. Comparing the final word against each of the last
    few heard words scores 0 there and 80 when Whisper merely mishears it as
    "cup", which is the distinction the rule needs.

    The second pass covers the final word being merged with its neighbour
    ("stand up" heard as "standup"), where a word-to-word comparison finds
    nothing but the bigram still matches.

    The search covers the WHOLE transcript, not its tail. Anchoring to the last
    few heard words is the obvious thing and it is wrong here: v644 pads a
    short line with real extra speech, so a five-word pad pushes the actual
    ending clean out of a five-word window and every padded clip reads as
    truncated. Searching everything trades that away for a different miss — a
    final word that also occurs earlier in the line will match its earlier self
    and the truncation goes unnoticed. That is the right way round to be wrong:
    this function can only ever VETO a rejection, so a miss costs a take that
    stays in the operator's queue, while the anchored version would have
    discarded good renders.
    """
    from rapidfuzz import fuzz
    if not ref_words:
        return True
    if not hyp_words:
        return False
    if max((fuzz.ratio(ref_words[-1], h) for h in hyp_words), default=0) >= threshold:
        return True
    needle = " ".join(list(ref_words)[-2:])
    return fuzz.partial_ratio(needle, " ".join(hyp_words)) >= threshold


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

HARD_KEYS = ("no_speech", "tail_truncated", "line_missing")

# The operator has RULED on these two (2026-08-23): "if i pick something that
# was cut was a mistake, good catch." So a clip they approved that turns out to
# be cut short is NOT a false reject — it is the checker doing its job and the
# approval being the error. Counting those as failures, which the first version
# of this file did, makes the machine look wrong for being right and pushes the
# thresholds in exactly the wrong direction.
CUT_REASONS = ("no_speech", "tail_truncated")

# They have NOT ruled on this one. `line_missing` fires when Veo says a
# different sentence that still delivers the whole idea, and they have approved
# that shape repeatedly. Disagreement here is a genuine open question about
# taste, not a scored error in either direction — so it is reported on its own
# line and never folded into a rate.
JUDGEMENT_REASONS = ("line_missing",)


def verdict(align: Dict[str, Any],
            asr: Dict[str, Any],
            tail_ok: bool,
            coverage_floor: float = COVERAGE_FLOOR,
            wer_ceiling: float = WER_CEILING,
            tail_fail_words: int = TAIL_FAIL_WORDS) -> Dict[str, Any]:
    """PASS / FAIL for one variant against one line, plus why.

    The hard list is short on purpose and every entry needs BOTH signals. See
    the module docstring: a rule that can fail a take on one instrument's
    opinion is the rule that rejected 44.5% of the operator's own image picks.
    """
    hard: List[str] = []
    warnings: List[str] = []

    heard_nothing = align["spoken_words"] == 0 or asr["hyp_words"] == 0
    if heard_nothing:
        hard.append("no_speech")

    # THE RULE THIS FILE EXISTS FOR. Three conditions, and all three must hold,
    # because each covers the way the other two lie:
    #
    #   1. the aligner could not place the final word(s) in the audio
    #   2. the transcript's edit path has no counterpart for them either
    #   3. the ending is not findable ANYWHERE in what was heard, fuzzily
    #
    # (1) alone over-fires on a rushed or swallowed ending. (2) alone over-fires
    # when one bad patch earlier in the line shoves the whole edit path out of
    # step. (3) is the guard that lets the count sit at one word: a final word
    # that Whisper merely misheard still scores high on a fuzzy search, so only
    # a genuinely absent ending gets this far.
    #
    # `min` of the two counts, not `max`: the more forgiving instrument sets the
    # number, so a take is discarded for the words BOTH agree are gone.
    #
    # The veto has a ceiling. It forgives ONE misheard final word; past
    # TAIL_VETO_LIMIT missing words the ending is plainly gone and a fuzzy
    # match on the final word means it simply occurs somewhere earlier in the
    # line, which is a property of the sentence and not evidence about the
    # audio.
    tail_n = min(align["tail_missing"], asr["tail_unheard"])
    if not heard_nothing and tail_n >= tail_fail_words and (
            not tail_ok or tail_n >= TAIL_VETO_LIMIT):
        hard.append("tail_truncated")

    # The whole line collapsed. Not the same failure as a clean cut: this is
    # "it said something else", and it needs both the alignment coverage and
    # the transcript to agree before it counts.
    if (not heard_nothing
            and align["coverage"] < coverage_floor
            and asr["wer"] > wer_ceiling):
        hard.append("line_missing")

    # --- warnings: true, recorded, and unable to fail anything -------------
    if align["tail_missing"] and "tail_truncated" not in hard:
        warnings.append(
            f"tail looks weak: last {align['tail_missing']} word(s) aligned poorly "
            f"but the transcript still contains the ending")
    if align["longest_missing_run"] >= 2 and "line_missing" not in hard:
        warnings.append(
            f"{align['longest_missing_run']} consecutive words mid-line aligned poorly: "
            f"{', '.join(align['missing'][:6])}")
    if align["ends_flush"] and "tail_truncated" not in hard:
        warnings.append(
            # ASCII only: this string is printed to a Windows console, where a
            # non-ASCII dash comes out as a replacement character.
            f"last word ends {align['tail_room_s']}s before the file does, "
            f"possible hard cut")
    if asr["inserted"] >= 3:
        warnings.append(
            f"{asr['inserted']} extra spoken words beyond the line "
            f"(expected when v644 dialogue_pad is in play)")
    if asr["substituted"]:
        warnings.append(
            f"heard differently: {', '.join(asr['substituted'][:6])}")
    if align["head_missing"]:
        warnings.append(f"first {align['head_missing']} word(s) aligned poorly")

    return {
        "verdict": "FAIL" if hard else "PASS",
        "hard": hard,
        "warnings": warnings,
        # The single number used to rank surviving takes. Coverage carries most
        # of it because a complete line is the thing being bought; the error
        # rate is a tiebreak and confidence settles the rest.
        "score": round(
            0.60 * align["coverage"]
            + 0.25 * max(0.0, 1.0 - asr["wer"])
            + 0.15 * align["mean_confidence"], 4),
    }


def score_variant(evidence: Dict[str, Any], **thresholds) -> Dict[str, Any]:
    """One cached variant -> one scored result. Pure.

    When the clip has both an A and a B line, both are scored and the better
    one wins, because `rendered_prompt_variant` is a per-clip column and the
    takes accumulate per attempt (see expected_lines). The winning line is
    named in the result so a surprising pass can be traced back.
    """
    align_kw = {k: thresholds[k] for k in ("conf_floor", "min_dur_s")
                if k in thresholds}
    verdict_kw = {k: thresholds[k] for k in
                  ("coverage_floor", "wer_ceiling", "tail_fail_words")
                  if k in thresholds}

    best: Optional[Dict[str, Any]] = None
    for cand in evidence.get("lines", []):
        align = score_alignment(cand.get("aligned_words") or [],
                                evidence.get("audio_duration") or 0.0,
                                **align_kw)
        ref = cand.get("ref_words") or []
        hyp = evidence.get("asr_words") or []
        asr = score_transcript(ref, hyp)
        tail_ok = tail_present_in_transcript(ref, hyp)
        v = verdict(align, asr, tail_ok, **verdict_kw)
        row = {
            "line_variant": cand.get("line_variant"),
            "line": cand.get("line"),
            "alignment": align,
            "transcript": {k: val for k, val in asr.items()
                           if k != "unheard_flags"},
            "tail_in_transcript": tail_ok,
            **v,
        }
        # Better = passes when the other does not, then higher score. A B-line
        # that merely scores higher never overturns an A-line that already
        # passed; it only rescues one that failed.
        if best is None:
            best = row
        elif (row["verdict"] == "PASS", row["score"]) > (best["verdict"] == "PASS", best["score"]):
            best = row

    if best is None:
        return {"verdict": "SKIP", "hard": [], "warnings": ["no line to score against"],
                "score": 0.0}
    return {
        "attempt": evidence.get("attempt"),
        "filename": evidence.get("filename"),
        "audio_duration": evidence.get("audio_duration"),
        "asr_text": " ".join(evidence.get("asr_words") or []),
        **best,
    }


def rank_variants(scored: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Passing takes first, then by score. Ties keep attempt order, so a rerun
    of the same evidence always produces the same ordering."""
    return sorted(
        scored,
        key=lambda r: (r.get("verdict") != "PASS", -float(r.get("score") or 0.0),
                       int(r.get("attempt") or 0)),
    )


# ============================================================================
# Backtest — the number that decides whether any of this may ever auto-discard
# ============================================================================

def agreement_stats(clips: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """How this rule compares with what the operator actually kept.

    `false_reject_rate` is the headline and the veto: the share of takes the
    operator selected that this rule would have thrown away. Anything but a
    very small number here means the thresholds are wrong, and image_qc's
    history says that is the likely outcome on the first pass, not a surprise.

    `pick_agreement` is secondary and deliberately so. Discarding broken takes
    is the job; agreeing with the operator's taste is not, and a checker judged
    on taste is a checker that starts failing takes for being less pretty.
    """
    considered = kept_failed = pick_match = 0
    approved = approved_failed = 0
    approved_caught_cut = approved_paraphrase = 0
    multi = 0
    # Clips whose chosen take is not among the ones we managed to score. These
    # MUST be counted and printed, not skipped. Measured on the first real run:
    # clip 14431 is `approved` with `selected_variant=3` while only attempts 1
    # and 2 exist in `versions_json`, so the old code hit `continue` and the
    # clip vanished from both the numerator and the denominator. A rate that
    # quietly drops the rows it cannot judge is the same failure as a checker
    # that caps its coverage and does not say so.
    unresolved = 0
    unresolved_approved = 0
    failed_reasons: Dict[str, int] = {}
    approved_reasons: Dict[str, int] = {}
    examples: List[Dict[str, Any]] = []
    for clip in clips:
        results = clip.get("results") or []
        if not results:
            continue
        chosen = clip.get("selected_variant")
        mine = rank_variants(results)
        if len(results) > 1:
            multi += 1
        picked = next((r for r in results if r.get("attempt") == chosen), None)
        if picked is None:
            unresolved += 1
            if clip.get("approval_status") == "approved":
                unresolved_approved += 1
            continue
        considered += 1
        # An APPROVED clip is the strong label. "Selected" can just mean nobody
        # got round to reviewing it, and in this corpus half the clips sit at
        # pending_review — counting those as endorsements would flatter the
        # rate. A take the operator explicitly approved and this rule discards
        # is a straight false reject with no room to argue.
        is_approved = clip.get("approval_status") == "approved"
        if is_approved:
            approved += 1
        if picked.get("verdict") == "FAIL":
            kept_failed += 1
            hard = picked.get("hard") or []
            for reason in hard:
                failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
            if is_approved:
                # Split by what the operator has actually ruled on. A cut clip
                # they approved is a CATCH; a paraphrase they approved is an
                # open question. Averaging the two produced a single "false
                # reject rate" that was wrong in both directions.
                if any(r in CUT_REASONS for r in hard):
                    approved_caught_cut += 1
                elif any(r in JUDGEMENT_REASONS for r in hard):
                    approved_paraphrase += 1
                approved_failed += 1
                for reason in hard:
                    approved_reasons[reason] = approved_reasons.get(reason, 0) + 1
            if len(examples) < 25:
                examples.append({
                    "clip_id": clip.get("clip_id"),
                    "approval_status": clip.get("approval_status"),
                    "line": clip.get("line"),
                    "heard": picked.get("asr_text"),
                    "hard": picked.get("hard"),
                    "coverage": picked.get("alignment", {}).get("coverage"),
                    "tail_missing": picked.get("alignment", {}).get("tail_missing"),
                    "wer": picked.get("transcript", {}).get("wer"),
                })
        if mine and mine[0].get("attempt") == chosen:
            pick_match += 1
    return {
        "clips_with_a_chosen_take": considered,
        "clips_with_multiple_takes": multi,
        "would_have_rejected_the_chosen_take": kept_failed,
        "false_reject_rate": round(kept_failed / considered, 4) if considered else None,
        "false_reject_reasons": failed_reasons,
        "approved_clips": approved,
        "approved_takes_flagged": approved_failed,
        # The operator's ruling, encoded: a cut clip they approved is a CATCH.
        "caught_a_cut_clip_you_approved": approved_caught_cut,
        # Still an open question, deliberately not a rate.
        "disagreed_on_a_paraphrase_you_approved": approved_paraphrase,
        "approved_flag_reasons": approved_reasons,
        "chosen_take_not_scored": unresolved,
        "chosen_take_not_scored_approved": unresolved_approved,
        "top_pick_matched_operator": pick_match,
        # Read this next to `clips_with_multiple_takes`. When a clip has ONE
        # take, our pick and theirs agree by construction, so agreement over
        # all clips is close to 1.0 no matter how good the ranking is. Only the
        # multi-take clips carry any information, and this corpus has almost
        # none of them — a rejected render is redone, not archived.
        "pick_agreement": round(pick_match / considered, 4) if considered else None,
        "false_reject_examples": examples,
    }


SWEEP_GRID = {
    "conf_floor": [0.25, 0.30, 0.40, 0.50],
    "coverage_floor": [0.60, 0.70, 0.75, 0.85],
    "wer_ceiling": [0.35, 0.50, 0.65],
    "tail_fail_words": [1, 2, 3],
}


def sweep(evidence_by_clip: Sequence[Dict[str, Any]],
          grid: Optional[Dict[str, List[Any]]] = None) -> List[Dict[str, Any]]:
    """Re-score the whole backtest at every threshold combination.

    Cheap because scoring is pure: the alignment and transcription ran once
    and live in the cache, so this is arithmetic over JSON. Sorted so the row
    at the top is the setting that rejects the fewest of the operator's own
    takes, breaking ties towards the setting that catches the most failures
    overall — a rule that rejects nothing has a perfect false-reject rate and
    is worthless.
    """
    import itertools
    grid = grid or SWEEP_GRID
    keys = list(grid)
    rows = []
    for combo in itertools.product(*(grid[k] for k in keys)):
        thresholds = dict(zip(keys, combo))
        rescored = []
        for clip in evidence_by_clip:
            rescored.append({
                "selected_variant": clip.get("selected_variant"),
                "approval_status": clip.get("approval_status"),
                "results": [score_variant(ev, **thresholds)
                            for ev in clip.get("evidence", [])],
            })
        stats = agreement_stats(rescored)
        total = sum(len(c["results"]) for c in rescored)
        failed = sum(1 for c in rescored for r in c["results"]
                     if r.get("verdict") == "FAIL")
        rows.append({
            **thresholds,
            "false_reject_rate": stats["false_reject_rate"],
            "caught_cut_approved": stats["caught_a_cut_clip_you_approved"],
            "paraphrase_disagreements": stats["disagreed_on_a_paraphrase_you_approved"],
            "pick_agreement": stats["pick_agreement"],
            "variants_failed": failed,
            "variants_total": total,
            "fail_rate": round(failed / total, 4) if total else None,
        })
    # Ranked on the APPROVED false-reject rate, then on catching the most.
    # A rule that discards nothing scores a perfect zero on the first key, so
    # the tiebreak has to pull the other way or the sweep recommends doing
    # nothing at all.
    # Rank on what is actually a cost. Catches are not a cost, so sorting on
    # them (as the first version did) rewarded a rule for MISSING cut clips the
    # operator had approved — the exact opposite of what they asked for.
    rows.sort(key=lambda r: (r["paraphrase_disagreements"],
                             -(r["caught_cut_approved"] or 0),
                             -(r["fail_rate"] or 0.0)))
    return rows


# ============================================================================
# The agreement ledger — what the machine called, and what the operator did
#
# The point of shadow mode is a number that grows: every clip the machine
# judged BEFORE the operator touched it, resolved against what they actually
# did. That number is the only thing that can ever justify letting this act on
# its own, and it cannot be reconstructed after the fact — once a clip is
# approved, you can no longer tell whether the machine called it first or was
# reading the answer over the operator's shoulder.
#
# So the ledger is append-only JSONL on disk, one row per clip per resolution,
# and it stores the machine's call together with the operator state AT THE TIME
# OF SCORING. Rows where that state was already `approved` are recorded but
# reported separately: they are the retrospective half and they flatter.
# ============================================================================

LEDGER_PATH = Path(os.path.expanduser("~")) / ".kaveno" / "clip-qc-agreement.jsonl"
# Every report is ALSO kept locally, whether or not it reached the server.
# Two reasons, and the second is the one that matters. The obvious one is that
# the endpoint may not be deployed yet, and the tracker should not have to wait
# for a deploy to start counting. The real one: a report stored only on the
# server can be overwritten by the next scoring run, and the ledger's whole
# claim is that the machine called it BEFORE the operator did. That claim needs
# a record written at call time that nothing later rewrites.
REPORT_DIR = Path(os.path.expanduser("~")) / ".kaveno" / "clip-qc-reports"


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def save_local_report(clip_id: Any, report: Dict[str, Any]) -> None:
    """Keep the FIRST report for a clip, not the latest.

    Deliberate: rescoring a clip the operator has since approved would
    otherwise silently replace a prospective call with a retrospective one and
    turn a real test into a flattering one. A later run may add takes, and
    those get a fresh report only after `_clear_clip_qc` invalidates this one
    (`--rescore` forces it).
    """
    if clip_id is None:
        return
    path = REPORT_DIR / f"{clip_id}.json"
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")


def load_local_report(clip_id: Any) -> Optional[Dict[str, Any]]:
    path = REPORT_DIR / f"{clip_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def ledger_rows(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Every row ever written. A corrupt line is skipped, not fatal."""
    path = path or LEDGER_PATH
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def ledger_append(rows: Sequence[Dict[str, Any]],
                  path: Optional[Path] = None) -> int:
    """Append resolved rows. Append-only on purpose: a ledger that can be
    rewritten is a ledger that can be quietly improved."""
    path = path or LEDGER_PATH
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(rows)


def resolve_clip(clip: Dict[str, Any], now: str) -> Optional[Dict[str, Any]]:
    """One clip with a stored report + a settled operator decision -> one row.

    Returns None while the answer is not in yet: no report, or the operator
    has not acted (`pending_review`). Counting an unreviewed clip as agreement
    would be counting silence as a yes.
    """
    qc = clip.get("qc")
    if not isinstance(qc, dict) or qc.get("version") != 1:
        return None
    status = clip.get("approval_status")
    if status not in ("approved", "rejected"):
        return None

    machine_verdict = qc.get("verdict")
    operator_kept = status == "approved"
    machine_kept = machine_verdict == "PASS"

    recommended = qc.get("recommended_attempt")
    chosen = clip.get("selected_variant")
    takes = qc.get("takes") or []
    # A pick only counts where there was something to pick BETWEEN. With one
    # take, machine and operator agree by construction and the row would
    # inflate the number without carrying information.
    pick_meaningful = len(takes) > 1

    return {
        "resolved_at": now,
        "clip_id": clip.get("id"),
        "job_id": clip.get("job_id"),
        "scored_at": qc.get("scored_at"),
        "checker": qc.get("checker"),
        # THE honest/flattering split: was the operator's decision already made
        # when the machine spoke?
        "prospective": qc.get("operator_state_at_scoring") == "pending_review",
        "machine_verdict": machine_verdict,
        "operator_status": status,
        "keep_agreement": machine_kept == operator_kept,
        "machine_kept": machine_kept,
        "operator_kept": operator_kept,
        "recommended_attempt": recommended,
        "operator_attempt": chosen,
        "pick_meaningful": pick_meaningful,
        "pick_agreement": (recommended == chosen) if pick_meaningful else None,
        "takes": len(takes),
        "hard": [h for t in takes if t.get("attempt") == chosen
                 for h in (t.get("hard") or [])],
        "line": (qc.get("line") or "")[:160],
    }


def scorecard(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """The running number, split so the flattering half cannot hide in it."""
    def block(subset):
        n = len(subset)
        if not n:
            return {"clips": 0}
        keep = sum(1 for r in subset if r.get("keep_agreement"))
        # The costly mistake, called by its name: the machine said discard and
        # the operator kept it. That is the number that gates ever acting.
        false_discard = sum(1 for r in subset
                            if r.get("operator_kept") and not r.get("machine_kept"))
        missed = sum(1 for r in subset
                     if not r.get("operator_kept") and r.get("machine_kept"))
        picks = [r for r in subset if r.get("pick_meaningful")]
        pick_ok = sum(1 for r in picks if r.get("pick_agreement"))
        return {
            "clips": n,
            "keep_agreement": round(keep / n, 4),
            "would_have_discarded_a_kept_clip": false_discard,
            "false_discard_rate": round(false_discard / n, 4),
            "missed_a_clip_you_rejected": missed,
            "clips_with_a_real_pick": len(picks),
            "pick_agreement": round(pick_ok / len(picks), 4) if picks else None,
        }

    # De-duplicate: a clip re-resolved on a later run must not count twice.
    latest: Dict[Any, Dict[str, Any]] = {}
    for r in rows:
        latest[r.get("clip_id")] = r
    uniq = list(latest.values())
    return {
        "total_rows": len(rows),
        "distinct_clips": len(uniq),
        "prospective": block([r for r in uniq if r.get("prospective")]),
        "retrospective": block([r for r in uniq if not r.get("prospective")]),
    }


def print_scorecard(card: Dict[str, Any]) -> None:
    print(f"\n=== AGREEMENT LEDGER ===  {card['distinct_clips']} distinct clips "
          f"({card['total_rows']} rows)")
    for name, label in (("prospective", "PROSPECTIVE (machine called it BEFORE you reviewed)"),
                        ("retrospective", "retrospective (you had already decided)")):
        b = card[name]
        print(f"\n  {label}")
        if not b["clips"]:
            print("    nothing resolved yet")
            continue
        print(f"    clips resolved           {b['clips']}")
        print(f"    keep/discard agreement   {b['keep_agreement']}")
        print(f"    would have discarded a clip you kept  "
              f"{b['would_have_discarded_a_kept_clip']}  -> rate {b['false_discard_rate']}")
        print(f"    missed a clip you rejected            "
              f"{b['missed_a_clip_you_rejected']}")
        if b["clips_with_a_real_pick"]:
            print(f"    clips with 2+ takes      {b['clips_with_a_real_pick']}"
                  f"   pick agreement {b['pick_agreement']}")
        else:
            print(f"    clips with 2+ takes      0  (no pick to agree on yet)")


# ============================================================================
# Removal — the first thing in this file that CHANGES anything
#
# One rule makes this defensible and it is not negotiable: THE MACHINE MAY ONLY
# TOUCH A CLIP THE OPERATOR HAS NOT DECIDED ON. `approved` and `rejected` are
# human decisions and are never overturned, whatever the checker thinks.
#
# That bounds the worst case precisely. On a clip nobody has reviewed, a wrong
# discard costs one re-render. Overturning an approval would cost a decision,
# and no measured rate here is good enough to buy that.
#
# The measured basis (313 clips, 151 approved, docs/experiments/
# clip-qc-2026-08-22.md): scoring would have failed 4 of 135 approved clips.
# Reading those four rather than counting them is what set the defaults below —
# one (clip 14274) is genuinely cut off mid-sentence at "and" and shipped
# anyway, so the rate is an upper bound that includes real catches.
# ============================================================================

# `line_missing` is NOT in the default, and that is a measured decision rather
# than caution. It is the reason that fires when the render says a DIFFERENT
# sentence — which is literally "the wrong dialogue" — but on real data almost
# every firing is Veo paraphrasing while still delivering the whole idea:
#
#   script: most brands cut corners with low-quality extracts or fillers.
#   heard : most companies water it down with cheap extracts and filler powders
#
# The operator has approved that kind of render repeatedly (clips 14283, 14313,
# 14302), so removing on paraphrase would fight their own revealed preference
# and burn renders re-rolling clips they would have kept. Add it back with
# `--reasons no_speech,tail_truncated,line_missing` if paraphrase should go too.
DEFAULT_DISCARD_REASONS = ("no_speech", "tail_truncated")

# `tail_truncated` fires at one missing final word, which is right for FLAGGING
# and too tight for REMOVING: a paraphrase that ends on a synonym ("never hear
# about" -> "never learn") scores tail_missing 1 and is perfectly usable. Two
# separates them on every example measured so far — the real truncations lose
# 2, 4 and 6 words and stop mid-word ("especially that f"), while the complete
# paraphrases all sit at exactly 1.
DEFAULT_MIN_TAIL_TO_REMOVE = 2

# A first run must not be able to sweep a whole account. Raise deliberately.
DEFAULT_DISCARD_CAP = 15


def discard_candidates(clips: Sequence[Dict[str, Any]],
                       reasons: Sequence[str] = DEFAULT_DISCARD_REASONS,
                       min_tail: int = DEFAULT_MIN_TAIL_TO_REMOVE,
                       include_approved_cuts: bool = False
                       ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(removable, protected). Pure, so the safety rule is unit-testable.

    `protected` is returned rather than silently dropped: a clip the checker
    failed but may not touch is exactly the thing the operator should see, and
    it is how "already approved but actually broken" surfaces at all.
    """
    removable, protected = [], []
    for clip in clips:
        qc = clip.get("qc")
        if not isinstance(qc, dict) or qc.get("version") != 1:
            continue
        if qc.get("verdict") != "FAIL":
            continue
        chosen = clip.get("selected_variant")
        takes = qc.get("takes") or []
        cur = next((t for t in takes if t.get("attempt") == chosen), None) or (
            takes[0] if takes else {})
        hard = [h for h in (cur.get("hard") or []) if h in reasons]
        # A one-word tail gap flags but does not remove: on real data that is
        # a paraphrase ending on a synonym, not a cut-off clip.
        if hard == ["tail_truncated"] and (cur.get("tail_missing") or 0) < min_tail:
            continue
        if not hard:
            continue
        row = {
            "clip_id": clip.get("id"),
            "job_id": clip.get("job_id"),
            "clip_index": clip.get("clip_index"),
            "status": clip.get("approval_status"),
            "hard": hard,
            "line": (qc.get("line") or "")[:150],
            "heard": (cur.get("heard") or "")[:150],
            "coverage": cur.get("coverage"),
            "tail_missing": cur.get("tail_missing"),
        }
        status = clip.get("approval_status")
        if status == "pending_review":
            removable.append(row)
        elif (include_approved_cuts and status == "approved"
              and any(h in CUT_REASONS for h in hard)):
            # Operator 2026-08-23: "if i pick something that was cut was a
            # mistake, good catch." So an APPROVED clip that turns out to be
            # cut may be sent back — but only for a CUT reason, and only when
            # asked for explicitly. A paraphrase they approved is their taste
            # and stays untouched no matter what flags are set.
            row["overriding_approval"] = True
            removable.append(row)
        else:
            protected.append(row)
    return removable, protected


def request_redo(session: Any, base: str, clip_id: int,
                 reason: str) -> Tuple[bool, str]:
    """Send the clip back to be re-rendered. Never raises."""
    try:
        resp = session.post(_url(base, f"/api/clips/{clip_id}/redo"),
                            json={"reason": reason}, timeout=120)
    except Exception as exc:
        return False, f"error {exc}"
    if resp.status_code == 200:
        return True, "redo queued"
    detail = ""
    try:
        detail = str(resp.json().get("detail", ""))[:120]
    except Exception:
        pass
    return False, f"{resp.status_code} {detail}"


def mark_rejected(session: Any, base: str, clip_id: int) -> Tuple[bool, str]:
    """Mark the clip rejected without re-rendering."""
    try:
        resp = session.post(_url(base, f"/api/clips/{clip_id}/reject"), timeout=120)
    except Exception as exc:
        return False, f"error {exc}"
    if resp.status_code == 200:
        return True, "rejected"
    detail = ""
    try:
        detail = str(resp.json().get("detail", ""))[:120]
    except Exception:
        pass
    return False, f"{resp.status_code} {detail}"


# ============================================================================
# Heavy layer — audio, alignment, transcription. Lazy imports throughout so
# the pure half above stays importable (and testable) on a box with no torch.
# ============================================================================

_ALIGNER: Optional[Dict[str, Any]] = None
_ASR = None


def _ensure_star_aligner() -> Dict[str, Any]:
    """MMS_FA loaded WITH the star dimension.

    transcript_alignment.py keeps its own singleton with `with_star=False` and
    is on the live export path, so it is left completely alone: a shadow-mode
    tool does not get to change how finished videos are trimmed. The weights
    are the same; only the output layer differs by one column. Not quantized
    here — this runs on the operator's box, not on Render's 2 GB dyno, and the
    unquantized model gives slightly better probabilities, which are the exact
    thing being measured.
    """
    global _ALIGNER
    if _ALIGNER is None:
        import torch
        from torchaudio.pipelines import MMS_FA as bundle
        model = bundle.get_model(with_star=True).to("cpu")
        model.eval()
        dictionary = bundle.get_dict()
        _ALIGNER = {
            "model": model,
            "tokenizer": bundle.get_tokenizer(),
            "aligner": bundle.get_aligner(),
            "star_id": dictionary.get("*"),
            "torch": torch,
        }
        if _ALIGNER["star_id"] is None:
            raise RuntimeError(
                "MMS_FA dictionary has no '*' token — this torchaudio build "
                "cannot do star alignment, which is the whole detector")
    return _ALIGNER


def _ensure_asr():
    global _ASR
    if _ASR is None:
        from faster_whisper import WhisperModel
        _ASR = WhisperModel(ASR_MODEL_ID, device="cpu", compute_type="int8",
                            cpu_threads=ASR_THREADS)
    return _ASR


def extract_audio(video_path: Path, wav_path: Path) -> bool:
    """mp4 -> mono 16 kHz wav, which is what both models want. Returns False
    rather than raising: one unreadable download is one variant we cannot
    score, not a lost run."""
    res = subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", str(video_path),
         "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", str(wav_path)],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        print(f"[clipqc] ffmpeg could not read {video_path.name}: "
              f"{(res.stderr or '').strip()[:200]}", flush=True)
        return False
    return wav_path.exists() and wav_path.stat().st_size > 0


def compute_emission(wav_path: Path):
    """The acoustic pass: audio -> per-frame token probabilities.

    Split out from the alignment because it is the expensive half and it does
    not depend on the script. A v821 clip has two candidate lines (A and B)
    and aligning each separately would run this twice over identical audio.
    Returns (emission, seconds_per_frame, audio_duration).
    """
    a = _ensure_star_aligner()
    torch = a["torch"]
    import torchaudio

    wav, sr = torchaudio.load(str(wav_path))
    if wav.shape[0] > 1:
        wav = wav.mean(dim=0, keepdim=True)
    if sr != 16000:
        wav = torchaudio.functional.resample(wav, sr, 16000)
        sr = 16000
    duration = float(wav.shape[-1]) / sr
    with torch.inference_mode():
        emission, _ = a["model"](wav)
    frame_s = (wav.shape[-1] / emission.shape[1]) / sr
    return emission[0], frame_s, duration


def align_words(emission_2d, frame_s: float,
                words: Sequence[str]) -> List[Dict[str, Any]]:
    """Force the expected words onto a precomputed emission, wrapped in stars.

    Targets are  * w1 w2 ... wn *  — see the module docstring for why the stars
    sit only at the ends. One row per script word: start, end, and the mean
    token probability that is the whole detection signal.
    """
    if not words:
        return []
    a = _ensure_star_aligner()
    spans = a["aligner"](emission_2d, a["tokenizer"](["*"] + list(words) + ["*"]))

    out: List[Dict[str, Any]] = []
    # spans[0] and spans[-1] are the stars; drop them, keep the real words.
    for text, group in zip(words, spans[1:-1]):
        if not group:
            out.append({"text": text, "start": 0.0, "end": 0.0, "confidence": 0.0})
            continue
        out.append({
            "text": text,
            "start": round(float(group[0].start * frame_s), 4),
            "end": round(float(group[-1].end * frame_s), 4),
            "confidence": round(float(sum(s.score for s in group) / len(group)), 4),
        })
    return out


def align_with_star(wav_path: Path,
                    words: Sequence[str]) -> Tuple[List[Dict[str, Any]], float]:
    """One-shot convenience: emission + alignment for a single line."""
    emission, frame_s, duration = compute_emission(wav_path)
    return align_words(emission, frame_s, words), duration


def transcribe(wav_path: Path) -> List[str]:
    """What the clip sounds like, with no expectations. Normalised into the
    same word shape as the script so the two are directly comparable."""
    model = _ensure_asr()
    segs, _info = model.transcribe(
        str(wav_path), language="en", beam_size=1,
        vad_filter=True, condition_on_previous_text=False,
    )
    text = " ".join((seg.text or "").strip() for seg in segs)
    return normalize_words(text)


# ============================================================================
# Evidence cache — the expensive pass runs once per rendered file, ever
# ============================================================================

def _cache_path(job_id: str, filename: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
    return CACHE_DIR / re.sub(r"[^A-Za-z0-9._-]", "_", job_id) / f"{safe}.json"


def load_evidence(job_id: str, filename: str) -> Optional[Dict[str, Any]]:
    path = _cache_path(job_id, filename)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if data.get("evidence_version") == EVIDENCE_VERSION else None


def save_evidence(job_id: str, filename: str, evidence: Dict[str, Any]) -> None:
    path = _cache_path(job_id, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=1),
                    encoding="utf-8")


def build_evidence(session: Any, base: str, job_id: str,
                   clip: Dict[str, Any], variant: Dict[str, Any],
                   force: bool = False) -> Optional[Dict[str, Any]]:
    """Download one take, listen to it both ways, cache the raw findings.

    The cache holds RAW measurements — per-word alignment rows and the
    transcript — never a verdict. Thresholds change; what the audio contains
    does not. That split is what makes the threshold sweep free.
    """
    filename = variant["filename"]
    if not force:
        cached = load_evidence(job_id, filename)
        if cached:
            return cached

    url = f"/api/jobs/{job_id}/outputs/{filename}"
    try:
        resp = session.get(_url(base, url), timeout=300)
    except Exception as exc:
        print(f"[clipqc] download failed for {filename}: {exc}", flush=True)
        return None
    if resp.status_code != 200:
        print(f"[clipqc] download returned {resp.status_code} for {filename}",
              flush=True)
        return None

    tmpdir = Path(tempfile.mkdtemp(prefix="clipqc_"))
    try:
        mp4 = tmpdir / filename.replace("/", "_")
        mp4.write_bytes(resp.content)
        wav = tmpdir / "audio.wav"
        if not extract_audio(mp4, wav):
            return None

        asr_words = transcribe(wav)
        emission, frame_s, duration = compute_emission(wav)
        lines = []
        for label, text in expected_lines(clip):
            ref = normalize_words(text)
            lines.append({"line_variant": label, "line": text, "ref_words": ref,
                          "aligned_words": align_words(emission, frame_s, ref)})

        evidence = {
            "evidence_version": EVIDENCE_VERSION,
            "job_id": job_id,
            "clip_id": clip.get("id"),
            "attempt": variant.get("attempt"),
            "filename": filename,
            "audio_duration": round(duration, 3),
            "asr_model": ASR_MODEL_ID,
            "asr_words": asr_words,
            "lines": lines,
        }
        save_evidence(job_id, filename, evidence)
        return evidence
    finally:
        for p in sorted(tmpdir.rglob("*"), reverse=True):
            try:
                p.unlink()
            except OSError:
                pass
        try:
            tmpdir.rmdir()
        except OSError:
            pass


# ============================================================================
# Network layer — reuses image_qc's token search wholesale. Two CLIs against
# the same server must not have two ideas of where the token lives.
# ============================================================================

def _auth_session(token: Optional[str] = None):
    from image_qc import _auth_session as _shared
    return _shared(token)


def _default_base_url() -> str:
    from image_qc import _default_base_url as _shared
    return _shared()


def _url(base: str, path: str) -> str:
    from image_qc import _url as _shared
    return _shared(base, path)


def build_report(clip: Dict[str, Any], results: Sequence[Dict[str, Any]],
                 scored_at: str) -> Dict[str, Any]:
    """The stored shape, version 1. Advisory: it recommends, never decides.

    `operator_state_at_scoring` is the field that makes the agreement loop
    honest, and it is easy to leave out. Scoring a clip the operator ALREADY
    approved measures nothing about whether the machine could have saved them
    the review — it only shows the machine agreeing with a decision it can see
    the result of. A report written while the clip still says `pending_review`
    is the prospective case: the operator had not acted yet, so what they do
    next is a real test. The agreement report splits on this field, and without
    it the two get averaged into one flattering number.
    """
    ranked = rank_variants(results)
    top = ranked[0] if ranked else None
    chosen = clip.get("selected_variant")
    current = next((r for r in results if r.get("attempt") == chosen), None)

    takes = []
    for r in ranked:
        a = r.get("alignment") or {}
        t = r.get("transcript") or {}
        takes.append({
            "attempt": r.get("attempt"),
            "verdict": r.get("verdict"),
            "score": r.get("score"),
            "hard": r.get("hard") or [],
            "warnings": (r.get("warnings") or [])[:4],
            "coverage": a.get("coverage"),
            "tail_missing": a.get("tail_missing"),
            # v939.6 — the two fields that tell "ran out of render time" apart
            # from "stopped early with time to spare". Without them a cut clip
            # cannot be diagnosed and every repair becomes a guess.
            "tail_room_s": a.get("tail_room_s"),
            "audio_duration": r.get("audio_duration"),
            "missing_words": (a.get("missing") or [])[:8],
            "wer": t.get("wer"),
            "line_variant": r.get("line_variant"),
            # Truncated on purpose: the report has a 64,000-byte cap and a
            # long line times several takes is the only thing here that grows.
            "heard": (r.get("asr_text") or "")[:400],
        })

    return {
        "version": 1,
        "checker": "v939",
        "scored_at": scored_at,
        "asr_model": ASR_MODEL_ID,
        # The clip-level call is about the take the operator is CURRENTLY
        # looking at, not about the best one available.
        "verdict": (current or top or {}).get("verdict"),
        "recommended_attempt": (top or {}).get("attempt"),
        "recommendation_reason": (
            "only take" if len(results) < 2 else
            f"best of {len(results)} takes: "
            f"{'passes' if (top or {}).get('verdict') == 'PASS' else 'least bad'}"
            f", score {(top or {}).get('score')}"),
        "selected_at_scoring": chosen,
        "operator_state_at_scoring": clip.get("approval_status"),
        "line": (clip.get("dialogue_text") or "")[:400],
        "takes": takes,
        "thresholds": {
            "word_conf_floor": WORD_CONF_FLOOR,
            "coverage_floor": COVERAGE_FLOOR,
            "wer_ceiling": WER_CEILING,
            "tail_fail_words": TAIL_FAIL_WORDS,
            "tail_veto_limit": TAIL_VETO_LIMIT,
        },
    }


def post_report(session: Any, base: str, clip_id: int,
                report: Dict[str, Any]) -> Tuple[bool, str]:
    """Send one report. Never raises — a clip that will not accept its report
    is one clip missing a badge, not a lost run. 409 is expected and normal:
    the clip started rendering again between scoring and posting."""
    try:
        resp = session.post(_url(base, f"/api/clips/{clip_id}/qc"),
                            json={"report": report}, timeout=120)
    except Exception as exc:
        return False, f"error {exc}"
    if resp.status_code == 200:
        return True, "ok"
    if resp.status_code == 409:
        return False, "409 clip is rendering again - rescore later"
    detail = ""
    try:
        detail = str(resp.json().get("detail", ""))[:120]
    except Exception:
        pass
    return False, f"{resp.status_code} {detail}"


def fetch_jobs(session: Any, base: str, limit: int = 50,
               since_days: int = 30) -> List[Dict[str, Any]]:
    """Recent jobs. `since_days` MUST be passed: the endpoint defaults it to 3
    (v726), so a backtest that omits it silently scores this week's work and
    reports a confident agreement number off four jobs. 0 disables the window.
    """
    resp = session.get(_url(base, "/api/jobs"),
                       params={"limit": limit, "since_days": since_days},
                       timeout=300)
    resp.raise_for_status()
    payload = resp.json()
    jobs = payload if isinstance(payload, list) else (payload.get("jobs") or [])
    return [j for j in jobs if isinstance(j, dict)]


def fetch_clips(session: Any, base: str, job_id: str) -> List[Dict[str, Any]]:
    resp = session.get(_url(base, f"/api/jobs/{job_id}/clips"), timeout=180)
    resp.raise_for_status()
    payload = resp.json()
    return [c for c in payload if isinstance(c, dict)] if isinstance(payload, list) else []


# ============================================================================
# Run one job
# ============================================================================

def score_job(session: Any, base: str, job_id: str,
              force: bool = False, post: bool = False) -> Dict[str, Any]:
    clips = fetch_clips(session, base, job_id)
    out_clips: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for clip in clips:
        ok, reason = should_score_clip(clip)
        if not ok:
            skipped.append({"clip_id": clip.get("id"),
                            "clip_index": clip.get("clip_index"),
                            "reason": reason})
            continue

        evidence_list = []
        for variant in variant_files(clip):
            ev = build_evidence(session, base, job_id, clip, variant, force=force)
            if ev:
                evidence_list.append(ev)
        if not evidence_list:
            skipped.append({"clip_id": clip.get("id"),
                            "clip_index": clip.get("clip_index"),
                            "reason": "no take could be downloaded or decoded"})
            continue

        results = [score_variant(ev) for ev in evidence_list]
        report = build_report(clip, results, _now_iso())
        # Local first, always: the ledger must not depend on a deploy, and the
        # call has to be recorded at the moment it was made.
        save_local_report(clip.get("id"), report)
        posted = None
        if post:
            ok, why = post_report(session, base, clip.get("id"), report)
            posted = why
            if not ok:
                print(f"[clipqc] clip {clip.get('id')} report not stored: {why}",
                      flush=True)
        out_clips.append({
            "clip_id": clip.get("id"),
            "clip_index": clip.get("clip_index"),
            "line": (clip.get("dialogue_text") or "").strip(),
            "selected_variant": clip.get("selected_variant"),
            "approval_status": clip.get("approval_status"),
            "results": results,
            "ranked": [r.get("attempt") for r in rank_variants(results)],
            "report": report,
            "posted": posted,
            "evidence": evidence_list,
        })

    return {"job_id": job_id, "clips": out_clips, "skipped": skipped}


def print_job_report(report: Dict[str, Any]) -> None:
    print(f"\n=== job {report['job_id']} ===")
    for clip in report["clips"]:
        line = clip["line"]
        print(f"\n  clip {clip['clip_index']} (id {clip['clip_id']})  "
              f"operator kept take {clip['selected_variant']}")
        print(f"    line: {line[:88]}{'...' if len(line) > 88 else ''}")
        for r in rank_variants(clip["results"]):
            mark = "FAIL" if r["verdict"] == "FAIL" else "pass"
            star = " <- operator's pick" if r.get("attempt") == clip["selected_variant"] else ""
            a = r["alignment"]
            print(f"    [{mark}] take {r['attempt']}  score {r['score']:.3f}  "
                  f"coverage {a['coverage']:.2f}  wer {r['transcript']['wer']:.2f}  "
                  f"tail_missing {a['tail_missing']}{star}")
            for h in r["hard"]:
                print(f"           HARD  {h}")
            for w in r["warnings"][:3]:
                print(f"           note  {w}")
            if r["verdict"] == "FAIL":
                print(f"           heard: {r['asr_text'][:100]}")
    for s in report["skipped"]:
        print(f"  - skipped clip {s['clip_index']}: {s['reason']}")


# ============================================================================
# CLI
# ============================================================================

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="clip_qc.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Check whether a rendered clip actually says its whole line.\n"
            "SHADOW MODE ONLY: this CLI has no write path. It never approves,\n"
            "rejects, selects or redoes anything."),
    )
    parser.add_argument("--job", action="append", default=[],
                        help="job id to score (repeatable)")
    parser.add_argument("--since-days", type=int, default=None,
                        help="score the most recent jobs instead of naming them")
    parser.add_argument("--limit", type=int, default=10,
                        help="max jobs when using --since-days")
    parser.add_argument("--backtest", action="store_true",
                        help="measure agreement with the takes the operator kept, "
                             "then sweep thresholds")
    parser.add_argument("--post", action="store_true",
                        help="store each report on its clip so the verdict shows "
                             "up in the review UI (still advisory: chooses nothing)")
    parser.add_argument("--resolve", action="store_true",
                        help="read back stored reports, resolve them against what "
                             "the operator did, append to the agreement ledger")
    parser.add_argument("--scorecard", action="store_true",
                        help="print the running agreement scorecard and exit")
    parser.add_argument("--discard", action="store_true",
                        help="find clips whose render does not say the line and "
                             "send them back to be re-rendered. DRY RUN unless "
                             "--apply. Never touches an approved clip.")
    parser.add_argument("--apply", action="store_true",
                        help="with --discard: actually do it")
    parser.add_argument("--reject-only", action="store_true",
                        help="with --discard: mark rejected instead of re-rendering")
    parser.add_argument("--reasons", default=",".join(DEFAULT_DISCARD_REASONS),
                        help="which hard reasons may trigger removal "
                             f"(default: {','.join(DEFAULT_DISCARD_REASONS)})")
    parser.add_argument("--include-approved-cuts", action="store_true",
                        dest="include_approved_cuts",
                        help="with --discard: also send back clips you already "
                             "APPROVED that turn out to be cut short. Only ever "
                             "applies to cut reasons, never to a paraphrase.")
    parser.add_argument("--min-tail", type=int, default=DEFAULT_MIN_TAIL_TO_REMOVE,
                        dest="min_tail",
                        help="with --discard: how many final words must be missing "
                             f"before tail_truncated removes (default "
                             f"{DEFAULT_MIN_TAIL_TO_REMOVE}; 1 flags a paraphrase "
                             f"that ends on a synonym)")
    parser.add_argument("--max", type=int, default=DEFAULT_DISCARD_CAP,
                        dest="max_discard",
                        help=f"cap on clips removed in one run (default {DEFAULT_DISCARD_CAP})")
    parser.add_argument("--force", action="store_true",
                        help="re-download and re-listen, ignoring the cache")
    parser.add_argument("--out", default=None, help="write the full report as JSON")
    parser.add_argument("--token", default=None)
    parser.add_argument("--url", default=None)
    args = parser.parse_args(argv)

    # Reading the ledger needs no server and no token.
    if args.scorecard:
        print_scorecard(scorecard(ledger_rows()))
        return 0

    base = (args.url or _default_base_url()).rstrip("/")
    try:
        session = _auth_session(args.token)
    except Exception as exc:
        print(f"[clipqc] {exc}", flush=True)
        return 3

    job_ids = list(args.job)
    if not job_ids:
        window = args.since_days if args.since_days is not None else 30
        jobs = fetch_jobs(session, base, limit=max(args.limit, 1),
                          since_days=window)
        job_ids = [j.get("id") for j in jobs if j.get("id")][:args.limit]
        print(f"[clipqc] no --job given; taking the {len(job_ids)} most recent "
              f"job(s) from the last {window or 'all'} days", flush=True)

    # --resolve reads stored reports back and settles them against what the
    # operator did. No audio, no models, no scoring: it is cheap and meant to
    # be run often, which is the whole point of a ledger that accumulates.
    if args.resolve:
        now = _now_iso()
        already = {r.get("clip_id") for r in ledger_rows()}
        fresh, waiting, unscored = [], 0, 0
        for job_id in job_ids:
            try:
                clips = fetch_clips(session, base, job_id)
            except Exception as exc:
                print(f"[clipqc] job {job_id}: {exc}", flush=True)
                continue
            for clip in clips:
                # Prefer the server's copy (it proves the report landed and is
                # what the operator actually saw), fall back to the local one
                # so the tracker works before the endpoint is deployed.
                if not isinstance(clip.get("qc"), dict):
                    local = load_local_report(clip.get("id"))
                    if local is None:
                        unscored += 1
                        continue
                    clip = {**clip, "qc": local}
                row = resolve_clip(clip, now)
                if row is None:
                    waiting += 1
                    continue
                if row["clip_id"] in already:
                    continue
                fresh.append(row)
        written = ledger_append(fresh)
        print(f"[clipqc] resolved {written} new clip(s); {waiting} scored but "
              f"still awaiting your decision; {unscored} not scored yet")
        print_scorecard(scorecard(ledger_rows()))
        return 0

    if args.discard:
        reasons = [r.strip() for r in args.reasons.split(",") if r.strip()]
        unknown = [r for r in reasons if r not in HARD_KEYS]
        if unknown:
            print(f"[clipqc] unknown reason(s): {unknown}. Valid: {list(HARD_KEYS)}")
            return 2
        removable, protected = [], []
        for job_id in job_ids:
            try:
                clips = fetch_clips(session, base, job_id)
            except Exception as exc:
                print(f"[clipqc] job {job_id}: {exc}", flush=True)
                continue
            # Attach the report (server copy first, local fallback) and the job
            # id, then let the pure function decide. Building the list here
            # rather than inline keeps the safety rule in one testable place.
            enriched = []
            for clip in clips:
                qc = clip.get("qc")
                if not isinstance(qc, dict):
                    qc = load_local_report(clip.get("id"))
                if qc is None:
                    continue
                enriched.append({**clip, "qc": qc,
                                 "job_id": clip.get("job_id") or job_id})
            rm, pr = discard_candidates(enriched, reasons, args.min_tail,
                                        args.include_approved_cuts)
            removable.extend(rm)
            protected.extend(pr)

        print(f"\n=== CLIPS WHOSE RENDER DOES NOT SAY THE LINE ===")
        print(f"reasons in play: {', '.join(reasons)}"
              f"   (tail_truncated removes at {args.min_tail}+ missing final words)")
        if protected:
            print(f"\n  {len(protected)} already-decided clip(s) also failed. NOT TOUCHED "
                  f"- a human decision is never overturned. Read them: if one of these\n"
                  f"  is genuinely broken, it already shipped.")
            for p in protected[:10]:
                print(f"    clip {p['clip_id']} [{p['status']}] {p['hard']}"
                      f"  coverage {p['coverage']}  tail_missing {p['tail_missing']}")
                print(f"      script: {p['line']}")
                print(f"      heard : {p['heard']}")

        if not removable:
            print("\n  nothing to remove: no unreviewed clip failed.")
            return 0

        capped = removable[:args.max_discard]
        print(f"\n  {len(removable)} unreviewed clip(s) would be removed"
              + (f" (capped to {len(capped)} this run; raise with --max)"
                 if len(capped) < len(removable) else ""))
        for r in capped:
            mark = "  [OVERRIDES YOUR APPROVAL]" if r.get("overriding_approval") else ""
            print(f"    clip {r['clip_id']} (job {str(r['job_id'])[:8]} #{r['clip_index']}) "
                  f"{r['hard']}  coverage {r['coverage']}  tail_missing {r['tail_missing']}{mark}")
            print(f"      script: {r['line']}")
            print(f"      heard : {r['heard']}")

        if not args.apply:
            print(f"\n  DRY RUN - nothing changed. Re-run with --apply to "
                  f"{'reject' if args.reject_only else 're-render'} these.")
            return 0

        done = failed = 0
        rows = []
        now = _now_iso()
        for r in capped:
            if args.reject_only:
                ok, why = mark_rejected(session, base, r["clip_id"])
            else:
                ok, why = request_redo(
                    session, base, r["clip_id"],
                    f"v939 clip QC: {', '.join(r['hard'])} - the render does not "
                    f"say the whole line")
            print(f"    clip {r['clip_id']}: {why}", flush=True)
            if ok:
                done += 1
                # Recorded as a MACHINE decision. Once we act, the operator's
                # counterfactual is gone forever — we will never know whether
                # they would have kept it — so these rows are kept apart from
                # the agreement ledger rather than counted as agreement.
                rows.append({"resolved_at": now, "clip_id": r["clip_id"],
                             "job_id": r["job_id"], "action":
                             "machine_reject" if args.reject_only else "machine_redo",
                             "hard": r["hard"], "line": r["line"],
                             "heard": r["heard"], "prospective": None,
                             "keep_agreement": None})
            else:
                failed += 1
        ledger_append(rows)
        print(f"\n  {done} removed, {failed} failed. Logged to {LEDGER_PATH}")
        return 0 if not failed else 1

    reports = []
    for job_id in job_ids:
        try:
            report = score_job(session, base, job_id, force=args.force,
                               post=args.post)
        except Exception as exc:
            print(f"[clipqc] job {job_id} failed: {exc}", flush=True)
            continue
        reports.append(report)
        print_job_report(report)

    all_clips = [c for r in reports for c in r["clips"]]
    total_variants = sum(len(c["results"]) for c in all_clips)
    failed = sum(1 for c in all_clips for r in c["results"] if r["verdict"] == "FAIL")
    print(f"\n[clipqc] {len(all_clips)} clips, {total_variants} takes, "
          f"{failed} would be discarded")

    payload: Dict[str, Any] = {"jobs": reports}

    if args.backtest:
        stats = agreement_stats(all_clips)
        print("\n=== BACKTEST ===")
        print(f"  clips with a chosen take       {stats['clips_with_a_chosen_take']}")
        print(f"  clips with 2+ takes            {stats['clips_with_multiple_takes']}")
        print(f"  would reject operator's pick   {stats['would_have_rejected_the_chosen_take']}"
              f"  -> false-reject rate {stats['false_reject_rate']}")
        print(f"  APPROVED clips                 {stats['approved_clips']}")
        print(f"  CAUGHT a cut clip you approved {stats['caught_a_cut_clip_you_approved']}"
              f"   <-- GOOD (operator 2026-08-23: 'good catch')")
        print(f"  disagreed on a paraphrase      {stats['disagreed_on_a_paraphrase_you_approved']}"
              f"   <-- open question, not scored as an error")
        if stats["approved_flag_reasons"]:
            print(f"  reasons on approved clips      {stats['approved_flag_reasons']}")
        print(f"  chosen take could NOT be scored {stats['chosen_take_not_scored']}"
              f" ({stats['chosen_take_not_scored_approved']} of them approved)"
              f" -- NOT in either rate above")
        print(f"  our top pick == theirs         {stats['pick_agreement']}"
              f"   (only {stats['clips_with_multiple_takes']} clips had a real "
              f"choice, so this number carries almost no information)")

        if stats["false_reject_examples"]:
            print("\n  every approved clip the checker flagged - READ THESE, a cut one "
                  "is a catch, not an error:")
            for ex in stats["false_reject_examples"][:6]:
                print(f"    clip {ex['clip_id']} [{ex['approval_status']}] {ex['hard']}")
                print(f"      line : {(ex['line'] or '')[:92]}")
                print(f"      heard: {(ex['heard'] or '')[:92]}")

        rows = sweep([{"selected_variant": c["selected_variant"],
                       "approval_status": c["approval_status"],
                       "evidence": c["evidence"]} for c in all_clips])
        print("\n  threshold sweep (fewest paraphrase disagreements, then most cut clips caught):")
        print("    conf  cov   wer   tailN | paraphrase-disagree  cut-caught  fails/total")
        for row in rows[:12]:
            print(f"    {row['conf_floor']:.2f}  {row['coverage_floor']:.2f}  "
                  f"{row['wer_ceiling']:.2f}  {row['tail_fail_words']}     | "
                  f"{row['paraphrase_disagreements']:>19}  "
                  f"{row['caught_cut_approved']:>10}  "
                  f"{row['variants_failed']}/{row['variants_total']}")
        payload["backtest"] = {"stats": stats, "sweep": rows}

    if args.out:
        Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1),
                                  encoding="utf-8")
        print(f"\n[clipqc] wrote {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
