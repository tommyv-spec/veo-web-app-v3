import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pairing_resolver import (  # noqa: E402
    PairingError,
    db_index_to_scene_no,
    resolve_audio_sources,
    scene_no_to_db_index,
    split_span,
)

# --- the base converters ---------------------------------------------------
# They live here so Phase 3a, the from-batch payload builder and the verifier
# share ONE definition. A private `- 1` in any of the three renders the wrong
# video instead of erroring, which is why the round-trip is asserted.
assert scene_no_to_db_index(1) == 0
assert db_index_to_scene_no(0) == 1
for _n in range(1, 30):
    assert db_index_to_scene_no(scene_no_to_db_index(_n)) == _n
for _i in range(0, 30):
    assert scene_no_to_db_index(db_index_to_scene_no(_i)) == _i


def scene(idx, speaker, audio_from=None, anchor=None, line=""):
    return {"scene_index": idx, "speaker_mode": speaker,
            "audio_from_scene": audio_from, "anchor_node_id": anchor, "line": line}


def expect_error(fn, needle):
    try:
        fn()
    except PairingError as exc:
        assert needle in str(exc), f"wrong error: {exc}"
        return
    raise AssertionError(f"expected PairingError containing {needle!r}")


# --- resolve_audio_sources -------------------------------------------------
s = [scene(1, "on-camera", line="i painted over four canvases last month."),
     scene(2, "voiceover", audio_from=1, line="i painted over four canvases"),
     scene(3, "voiceover", audio_from=1, line="last month.")]
out = resolve_audio_sources(s)
assert out[2]["audio_source_scene"] == 1 and out[2]["mint_twin"] is False
assert out[3]["audio_source_scene"] == 1 and out[3]["mint_twin"] is False

out = resolve_audio_sources([scene(1, "voiceover", anchor=5221, line="a line")])
assert out[1]["mint_twin"] is True and out[1]["audio_source_scene"] is None

expect_error(lambda: resolve_audio_sources(
    [scene(1, "on-camera", line="x"),
     scene(2, "voiceover", audio_from=1, anchor=5221, line="a")]),
    "both")
expect_error(lambda: resolve_audio_sources([scene(1, "voiceover", audio_from=9, line="a")]),
             "no scene 9")
expect_error(lambda: resolve_audio_sources(
    [scene(1, "silent"), scene(2, "voiceover", audio_from=1, line="a")]),
    "does not speak")
# a pairing may not chain through another voiceover scene
expect_error(lambda: resolve_audio_sources(
    [scene(1, "on-camera", line="x"),
     scene(2, "voiceover", audio_from=1, line="a"),
     scene(3, "voiceover", audio_from=2, line="b")]),
    "does not speak")

# a PHRASE speaker resolves through its tokens - builds really write this, and
# a bare-token-only resolver rejected the first real build to use the feature
s = [scene(1, "the main character on-camera", line="a spoken line"),
     scene(2, "voiceover", audio_from=1, line="a spoken")]
out = resolve_audio_sources(s)
assert out[2]["audio_source_scene"] == 1, out

# voiceover beats on-camera in a phrase, matching the platform's priority
expect_error(lambda: resolve_audio_sources(
    [scene(1, "the main character voiceover", line="x"),
     scene(2, "voiceover", audio_from=1, line="a")]),
    "does not speak")

# a silent phrase is not a source either
expect_error(lambda: resolve_audio_sources(
    [scene(1, "the guides, silent", line=""),
     scene(2, "voiceover", audio_from=1, line="a")]),
    "does not speak")

# --- split_span ------------------------------------------------------------
# one sharer takes the whole span - this is why a one-visual group needs no
# special case anywhere in the export
assert split_span(10.0, 14.0, ["abcd"]) == [(10.0, 14.0)]

# two equal fragments split it in half
assert split_span(10.0, 14.0, ["abcd", "efgh"]) == [(10.0, 12.0), (12.0, 14.0)]

# weighting is by character length; windows are contiguous, ordered,
# non-overlapping, and end exactly on the parent's end
spans = split_span(0.0, 9.0, ["a", "bb", "cccccc"])
assert spans[0][0] == 0.0
assert spans[-1][1] == 9.0
for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
    assert a1 == b0, "windows must be contiguous"
    assert a0 < a1, "each window must be non-empty"
assert abs((spans[0][1] - spans[0][0]) - 1.0) < 1e-6, spans

# an empty fragment still gets a real window, never a zero-length one
spans = split_span(0.0, 3.0, ["", "abc"])
assert spans[0][1] > spans[0][0]
assert spans[-1][1] == 3.0

expect_error(lambda: split_span(5.0, 5.0, ["a"]), "not positive")
expect_error(lambda: split_span(0.0, 1.0, []), "no fragments")

print("check_pairing_resolver: OK")
