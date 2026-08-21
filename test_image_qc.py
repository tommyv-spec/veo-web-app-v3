import numpy as np
import cv2

from image_qc import analyze_integrity


def _png(arr):
    ok, buf = cv2.imencode(".png", arr)
    assert ok
    return bytes(buf)


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
    # measured even though the blur REASON is not appended for a blank frame
    assert r["metrics"]["lap_var"] is not None
    assert r["metrics"]["gray_std"] == 0.0


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
    assert "undecodable" in r["reasons"]
    assert r["metrics"] is None


def test_integrity_empty_bytes():
    """OpenCV asserts on a zero-length buffer instead of returning None.
    A failed download must not abort the whole batch run."""
    r = analyze_integrity(b"")
    assert r["ok"] is False
    assert r["reasons"] == ["undecodable"]
    assert r["metrics"] is None
