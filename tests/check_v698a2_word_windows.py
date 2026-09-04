"""v698A.2 — cutaways placed on their spoken words: the pure helpers, and one
end-to-end pass through the REAL v825 resolver on garnissa v4's sentence 7.

Run: python tests/check_v698a2_word_windows.py  (from code/)
"""
import os
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pairing_resolver import (  # noqa: E402
    build_alignment_inputs,
    sentence_container,
    tile_fragment_windows,
)
import video_processor as vp  # noqa: E402


def close(a, b, tol=1e-6):
    return abs(float(a) - float(b)) <= tol


# --- tile_fragment_windows ---------------------------------------------------
w, r = tile_fragment_windows((10.0, 18.0), [10.0, 12.5, 15.0])
assert r is None and len(w) == 3, (w, r)
assert close(w[0][0], 10.0) and close(w[0][1], 12.5)
assert close(w[1][0], 12.5) and close(w[1][1], 15.0)
assert close(w[2][0], 15.0) and close(w[2][1], 18.0)
assert w[0][1] == w[1][0] and w[1][1] == w[2][0], "windows must tile with no hole"

w, r = tile_fragment_windows((10.0, 18.0), [10.0])
assert r is None and w == [(10.0, 18.0)], "a lone fragment takes the whole container"

w, r = tile_fragment_windows((10.0, 18.0), [10.0, None, 15.0])
assert w is None and r == "unresolved fragment 2", r

w, r = tile_fragment_windows((10.0, 18.0), [10.0, 15.0, 12.5])
assert w is None and r == "non-monotonic", r

w, r = tile_fragment_windows((10.0, 18.0), [10.0, 9.5, 15.0])
assert w is None and r.startswith("boundary 2 outside sentence window by 0.50s"), r

w, r = tile_fragment_windows((10.0, 18.0), [10.0, 12.5, 19.0])
assert w is None and r.startswith("boundary 3 outside sentence window by 1.00s"), r

w, r = tile_fragment_windows((10.0, 18.0), [10.0, 12.5, 17.9])
assert w is None and r == "non-monotonic", "last boundary within 0.15s of the end"

w, r = tile_fragment_windows((10.0, 18.0), [])
assert w is None and r == "no fragments"
print("OK tile_fragment_windows")

# --- sentence_container ------------------------------------------------------
env = (10.0, 18.0)
c, r = sentence_container(env, 9.4, 18.1, 17.7, prev_lo=3, this_lo=12)
assert r is None and close(c[0], 9.4) and close(c[1], 18.1), (c, r)   # words win over the envelope
c, r = sentence_container(env, 8.8, 18.1, 17.7, prev_lo=3, this_lo=12)
assert c is None and r.startswith("aligned sentence drifts from mapped window by 1.20s (start)"), r
c, r = sentence_container(env, 10.1, 19.3, 17.7, prev_lo=3, this_lo=12)
assert c is None and "1.30s (end, next line)" in r, r
c, r = sentence_container(env, 10.1, 18.1, 17.7, prev_lo=12, this_lo=12)
assert c is None and r == "scene alignment did not advance", r
c, r = sentence_container(env, 10.1, 18.1, 17.7, prev_lo=12, this_lo=5)
assert c is None and r == "scene alignment did not advance", r
c, r = sentence_container(env, 10.1, 18.1, 17.7, prev_lo=None, this_lo=0)
assert r is None, "first slot has no previous lo to advance past"
c, r = sentence_container(env, 10.1, 18.1, 17.7, prev_lo=3, this_lo=None)
assert c is None and r == "sentence line did not align", r
c, r = sentence_container(env, None, 18.1, 17.7, prev_lo=3, this_lo=12)
assert c is None and r == "first fragment unresolved", r
# end preference: next line start > last word end > envelope end
c, r = sentence_container(env, 10.1, None, 17.7, prev_lo=3, this_lo=12)
assert r is None and close(c[1], 17.7), (c, r)
c, r = sentence_container(env, 10.1, None, None, prev_lo=3, this_lo=12)
assert r is None and close(c[1], 18.0), (c, r)
c, r = sentence_container(env, 10.1, 10.2, None, prev_lo=3, this_lo=12)
assert c is None and r.startswith("aligned sentence drifts"), r   # 7.8s short of the envelope end
print("OK sentence_container")

# --- v948 keep-list mapping: a hole BEFORE and a hole INSIDE a sentence -------
# kept ranges in pre-sweep time; holes 5-6s and 12-13s are cut out.
keeps = [(0.0, 5.0), (6.0, 12.0), (13.0, 20.0)]
m = vp.map_time_through_keep_segments
assert close(m(7.0, keeps), 6.0), m(7.0, keeps)      # 1s hole before -> shifts by exactly 1s
assert close(m(14.0, keeps), 12.0), m(14.0, keeps)   # two 1s holes before -> shifts by exactly 2s
assert close(m(12.5, keeps), 11.0), m(12.5, keeps)   # inside a hole -> collapses onto the cut
win = (m(7.0, keeps), m(14.0, keeps))
assert close(win[1] - win[0], 6.0), "7s window with a 1s hole inside -> 6s, subtracted, never scaled"
print("OK v948 keep-list mapping (hole before + hole inside)")

# --- build_alignment_inputs --------------------------------------------------
slot_texts = [
    ("a hundred sets are free for garnissa's first anniversary.", []),
    ("so i made the acrylic guides, over a thousand pages that walk you through it and give you somewhere to write down what worked.",
     ["so i made the acrylic guides, over a thousand pages that walk you through it and give you somewhere to write down what worked.",
      "so i made the acrylic guides - over a thousand pages that walk you through it"]),
]
groups = [(1, ["So I made the acrylic guides,", "over a thousand pages",
               "that walk you through it and give you somewhere to write down what worked."]),
          (0, ["a hundred sets are free for garnissa's first anniversary."]),
          (0, ["", "x"])]
scene_lines, inserts, index, skipped = build_alignment_inputs(groups, slot_texts, vp._normalize)
assert len(scene_lines) == 2
assert scene_lines[0]["candidates"] == [slot_texts[0][0]], scene_lines[0]
assert scene_lines[1]["candidates"][0] == slot_texts[1][0] and len(scene_lines[1]["candidates"]) == 2
assert len(inserts) == 4, inserts
assert inserts[0]["start_word"] == "so" and inserts[0]["end_word"] == "guides", inserts[0]   # capital + comma gone
assert inserts[1]["start_word"] == "over" and inserts[1]["end_word"] == "pages"
assert inserts[2]["start_word"] == "that" and inserts[2]["end_word"] == "worked"
assert inserts[3]["start_word"] == "a" and inserts[3]["end_word"] == "anniversary"
assert inserts[3]["phrase"].endswith("garnissa's first anniversary.")   # phrase stays verbatim
assert vp._normalize("garnissa's") == "garnissas"                           # apostrophe: same rule both sides
assert [i["support_index"] for i in inserts] == [1, 2, 3, 4]
assert index == {1: (0, 1), 2: (0, 2), 3: (0, 3), 4: (1, 1)}, index
assert skipped == {2: "empty fragment 1"}, skipped
assert [i["image_index"] for i in inserts] == [1, 1, 1, 0]
print("OK build_alignment_inputs")

# --- END TO END through the REAL v825 resolver: garnissa v4 sentence 7 -------
# Build lines 405-415: the spoken line and its three fragments (scenes 20/21/22).
LINE_BEFORE = "a hundred sets are free for garnissa's first anniversary."
LINE_7 = ("so i made the acrylic guides, over a thousand pages that walk you "
          "through it and give you somewhere to write down what worked.")
FRAGS_7 = ["so i made the acrylic guides,", "over a thousand pages",
           "that walk you through it and give you somewhere to write down what worked."]
LINE_AFTER = "and the first hundred are free."
assert vp._normalize(" ".join(FRAGS_7)) == vp._normalize(LINE_7), "fragments must rebuild the sentence"


def synth_master(lines, t0=30.0, step=0.35, dur=0.30, drop=None, replace=None):
    """One heard word every `step` seconds, `dur` long. `drop` removes a word
    (its time is NOT kept - the following words move up), `replace` swaps one
    heard word's text (a mishearing)."""
    words, t = [], t0
    for line in lines:
        for w in vp._normalize(line).split():
            if drop and w == drop:
                continue
            heard = replace.get(w, w) if replace else w
            words.append({"word": heard, "start": round(t, 3), "end": round(t + dur, 3)})
            t += step
    return words


def run_sentence_7(master, env):
    slot_texts = [(LINE_BEFORE, []), (LINE_7, []), (LINE_AFTER, [])]
    groups = [(1, FRAGS_7)]
    scene_lines, inserts, index, skipped = build_alignment_inputs(groups, slot_texts, vp._normalize)
    assert not skipped
    master_text = " ".join(vp._normalize(w["word"]) for w in master)
    aligned = vp._align_scene_lines(master, master_text, scene_lines)
    spans = vp.resolve_support_spans(master, inserts, scene_lines)
    assert all(s is not None for s in spans), spans
    lo_prev = aligned[0]["lo"] if aligned[0] else None
    lo_this = aligned[1]["lo"] if aligned[1] else None
    next_start = master[aligned[2]["lo"]]["start"] if aligned[2] else None
    cont, r = sentence_container(env, spans[0]["start"], next_start, spans[-1]["end"], lo_prev, lo_this)
    assert r is None, r
    starts = [s["start"] for s in spans]
    wins, r = tile_fragment_windows(cont, starts)
    assert r is None, r
    return wins, spans, cont


# clean hearing: every word exactly as written.
# words of sentence 7 start at 30 + 9*0.35 (9 words in LINE_BEFORE) = 33.15
master = synth_master([LINE_BEFORE, LINE_7, LINE_AFTER])
t_s7 = 30.0 + 9 * 0.35
t_over = t_s7 + 6 * 0.35      # so i made the acrylic guides | OVER
t_that = t_s7 + 10 * 0.35     # over a thousand pages | THAT
t_after = t_s7 + 24 * 0.35    # first word of the next line
env = (t_s7 - 0.1, t_after - 0.05)   # the mapped window, slightly off on purpose
wins, spans, cont = run_sentence_7(master, env)
assert close(cont[0], t_s7, 1e-3) and close(cont[1], t_after, 1e-3), cont   # edges from the words
assert close(wins[1][0], t_over, 1e-3), (wins, t_over)   # boundary ON "over"
assert close(wins[2][0], t_that, 1e-3), (wins, t_that)   # boundary ON "that"
assert wins[0][1] == wins[1][0] and wins[1][1] == wins[2][0] and close(wins[2][1], t_after, 1e-3)
assert all(s["confidence"] == 1.0 for s in spans), spans
print("OK sentence 7 end-to-end: boundaries on 'over' and 'that', edges from the words")

# SYNTHETIC: "over" misheard as "ova" -> still lands (fuzzy or proportional), never falls back.
master = synth_master([LINE_BEFORE, LINE_7, LINE_AFTER], replace={"over": "ova"})
wins, spans, cont = run_sentence_7(master, env)
assert close(wins[1][0], t_over, 1e-3), (wins, t_over)
assert spans[1]["confidence"] <= 1.0
print("OK sentence 7 synthetic: misheard 'over' -> 'ova' still lands on its slot")

# SYNTHETIC: "that" missing entirely -> proportional placement, inside the container, monotonic.
master = synth_master([LINE_BEFORE, LINE_7, LINE_AFTER], drop="that")
t_after_drop = t_after - 0.35
env_drop = (t_s7 - 0.1, t_after_drop - 0.05)
wins, spans, cont = run_sentence_7(master, env_drop)
assert cont[0] <= wins[2][0] <= cont[1], wins
assert wins[0][0] < wins[1][0] < wins[2][0] < wins[2][1], wins
assert spans[2]["confidence"] < 1.0, spans[2]
print("OK sentence 7 synthetic: missing 'that' -> proportional, inside the sentence, monotonic")

print("ALL OK check_v698a2_word_windows")
