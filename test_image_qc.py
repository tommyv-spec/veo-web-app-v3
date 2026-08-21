import numpy as np
import cv2

from image_qc import analyze_integrity, build_judge_prompt, parse_judge_reply


def _png(arr):
    ok, buf = cv2.imencode(".png", arr)
    assert ok
    return bytes(buf)


def _noise(sigma, seed=5):
    """Mid-grey plus controlled per-pixel noise of a known standard deviation.
    The same single channel is replicated to B/G/R so the BGR2GRAY weights
    (0.299 + 0.587 + 0.114 = 1.0) reproduce it exactly — the measured gray_std
    lands on `sigma`. Per-pixel noise is high-frequency, so lap_var stays far
    above the blur floor and only the blank gate is under test."""
    rng = np.random.default_rng(seed)
    chan = np.clip(rng.normal(128.0, sigma, (1024, 576)), 0, 255).astype(np.uint8)
    return np.repeat(chan[:, :, None], 3, axis=2)


def _blocks(seed=3):
    """Big colour blocks: a real render has low-frequency content plus hard
    edges. Nearest-neighbour keeps the edges, so this is SHARP."""
    rng = np.random.default_rng(seed)
    blocks = rng.integers(0, 255, (18, 32, 3), dtype=np.uint8)
    return cv2.resize(blocks, (576, 1024), interpolation=cv2.INTER_NEAREST)


def test_integrity_passes_normal_image():
    rng = np.random.default_rng(7)
    img = rng.integers(0, 255, (1024, 576, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is True
    assert r["reasons"] == []
    assert set(r["metrics"]) == {"short_side", "gray_std", "lap_var"}
    assert r["metrics"]["short_side"] == 576


def test_integrity_flags_blank_frame():
    img = np.full((1024, 576, 3), 12, dtype=np.uint8)  # near-black, no variance
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert r["reasons"] == ["blank_frame"]
    # measured even though the blur REASON is not appended for a blank frame:
    # a perfectly uniform frame has a Laplacian of all zeros
    assert r["metrics"]["lap_var"] == 0.0
    assert r["metrics"]["gray_std"] == 0.0


def test_integrity_blank_boundary_std5_fails():
    """Lower bracket for the blank gate (floor 8.0): a barely-varying frame
    is still junk."""
    r = analyze_integrity(_png(_noise(5.0)))
    assert r["ok"] is False
    assert r["reasons"] == ["blank_frame"]
    assert r["metrics"]["gray_std"] < 8.0


def test_integrity_blank_boundary_std11_passes():
    """Upper bracket: real renders sit well above the floor, and the gate must
    not creep up into them."""
    r = analyze_integrity(_png(_noise(11.0)))
    assert r["ok"] is True
    assert r["reasons"] == []
    assert r["metrics"]["gray_std"] > 8.0


def test_integrity_flags_tiny_resolution():
    img = np.random.default_rng(1).integers(0, 255, (64, 36, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert r["reasons"] == ["low_resolution"]


def test_integrity_resolution_boundary_255_fails():
    img = np.random.default_rng(21).integers(0, 255, (400, 255, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert r["reasons"] == ["low_resolution"]
    assert r["metrics"]["short_side"] == 255


def test_integrity_resolution_boundary_257_passes():
    img = np.random.default_rng(22).integers(0, 255, (400, 257, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is True
    assert r["reasons"] == []
    assert r["metrics"]["short_side"] == 257


def test_integrity_passes_sharp_low_frequency_image():
    """A sharp image made of big flat colour areas must NOT read as blurred.
    Guards the blur gate against being tightened until it eats real renders."""
    r = analyze_integrity(_png(_blocks()))
    assert r["ok"] is True
    assert r["reasons"] == []
    assert r["metrics"]["lap_var"] > 100.0


def test_integrity_flags_extreme_blur():
    # Blur the block fixture, not white noise: white noise has no
    # low-frequency content, so blurring it flattens the frame entirely and
    # trips blank_frame instead. Blocks keep their tonal variance (std ~17.9)
    # while the edges are destroyed (lap_var ~2.3).
    img = cv2.GaussianBlur(_blocks(), (0, 0), sigmaX=25)
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert r["reasons"] == ["extreme_blur"]


def test_integrity_undecodable_bytes():
    r = analyze_integrity(b"not an image at all")
    assert r["ok"] is False
    assert r["reasons"] == ["undecodable"]
    assert r["metrics"] is None


def test_integrity_empty_bytes():
    """OpenCV asserts on a zero-length buffer instead of returning None.
    A failed download must not abort the whole batch run."""
    r = analyze_integrity(b"")
    assert r["ok"] is False
    assert r["reasons"] == ["undecodable"]
    assert r["metrics"] is None


# ── Gemini checklist judge: prompt builder + tolerant reply parser ──────────
# Pure functions only — no test here touches the network.


def test_judge_prompt_embeds_spec_and_bans():
    p = build_judge_prompt("A woman in a cobalt dress holds a brass scale, kitchen.")
    assert "cobalt dress" in p
    assert "lab coat" in p.lower()          # §8 compliance rows always present
    assert "child" in p.lower()             # v808
    assert "json" in p.lower()


def test_parse_judge_reply_happy_path():
    raw = '{"overall": 7, "verdict": "pass", "element_misses": [], '\
          '"artifacts": ["warped left hand"], "compliance": [], "reasons": ["ok"]}'
    r = parse_judge_reply(raw)
    assert r["overall"] == 7
    assert r["verdict"] == "pass"
    assert r["artifacts"] == ["warped left hand"]


def test_parse_judge_reply_strips_code_fence():
    raw = '```json\n{"overall": 3, "verdict": "fail", "element_misses": ["no scale"], '\
          '"artifacts": [], "compliance": [], "reasons": []}\n```'
    assert parse_judge_reply(raw)["verdict"] == "fail"


def test_parse_judge_reply_compliance_forces_fail():
    raw = '{"overall": 9, "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": ["stethoscope visible"], "reasons": []}'
    r = parse_judge_reply(raw)
    assert r["verdict"] == "fail"           # a compliance hit can never pass


def test_parse_judge_reply_garbage_returns_none():
    assert parse_judge_reply("the image is nice") is None


def test_parse_judge_reply_clamps_overall():
    raw = '{"overall": 99, "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": [], "reasons": []}'
    assert parse_judge_reply(raw)["overall"] == 10
    raw2 = '{"overall": -3, "verdict": "pass", "element_misses": [], '\
           '"artifacts": [], "compliance": [], "reasons": []}'
    assert parse_judge_reply(raw2)["overall"] == 0


def test_parse_judge_reply_non_numeric_overall_returns_none():
    raw = '{"overall": "high", "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": [], "reasons": []}'
    assert parse_judge_reply(raw) is None


def test_parse_judge_reply_never_raises_on_junk():
    """Every decision downstream reads this dict, so the parser is only allowed
    two answers: a normalised dict, or None. It may never throw."""
    for bad in (None, "", "   ", b"bytes not str", "{", "}{", "{not json}",
                "[1, 2, 3]", '{"no_overall": 1}', '{"overall": null}',
                '{"overall": true}', '{"overall": [1]}', '{"overall": "Infinity"}',
                '{"overall": 5, "artifacts": 7}',
                '{"overall": 5, "compliance": "stethoscope"}'):
        assert parse_judge_reply(bad) is None or isinstance(parse_judge_reply(bad), dict)


def test_parse_judge_reply_rejects_bool_overall():
    """JSON `true` is an int subclass in Python — it must not read as 1."""
    raw = '{"overall": true, "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": [], "reasons": []}'
    assert parse_judge_reply(raw) is None


def test_parse_judge_reply_scalar_list_field_is_wrapped():
    """A model that answers a list field with a bare string must not explode
    into one 'reason' per character."""
    raw = '{"overall": 5, "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": "stethoscope on the table", "reasons": []}'
    r = parse_judge_reply(raw)
    assert r["compliance"] == ["stethoscope on the table"]
    assert r["verdict"] == "fail"


def test_parse_judge_reply_float_overall_truncates():
    raw = '{"overall": 7.9, "verdict": "pass", "element_misses": [], '\
          '"artifacts": [], "compliance": [], "reasons": []}'
    assert parse_judge_reply(raw)["overall"] == 7


def test_parse_judge_reply_missing_verdict_defaults_pass():
    """verdict is recomputed, never trusted: absent + no compliance = pass."""
    raw = '{"overall": 6, "element_misses": [], "artifacts": [], '\
          '"compliance": [], "reasons": []}'
    assert parse_judge_reply(raw)["verdict"] == "pass"
