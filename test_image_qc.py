import numpy as np
import cv2

from image_qc import analyze_integrity


def _png(arr):
    ok, buf = cv2.imencode(".png", arr)
    assert ok
    return bytes(buf)


def test_integrity_passes_normal_image():
    rng = np.random.default_rng(7)
    img = rng.integers(0, 255, (1024, 576, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is True
    assert r["reasons"] == []


def test_integrity_flags_blank_frame():
    img = np.full((1024, 576, 3), 12, dtype=np.uint8)  # near-black, no variance
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert "blank_frame" in r["reasons"]


def test_integrity_flags_tiny_resolution():
    img = np.random.default_rng(1).integers(0, 255, (64, 36, 3), dtype=np.uint8)
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert "low_resolution" in r["reasons"]


def test_integrity_flags_extreme_blur():
    # Big colour blocks, not white noise: a real render has low-frequency
    # content, so blurring it kills the edges (Laplacian) while the frame
    # still has plenty of tonal variance (std). Pure noise blurs to a flat
    # grey and would trip blank_frame instead.
    rng = np.random.default_rng(3)
    blocks = rng.integers(0, 255, (18, 32, 3), dtype=np.uint8)
    img = cv2.resize(blocks, (576, 1024), interpolation=cv2.INTER_NEAREST)
    img = cv2.GaussianBlur(img, (0, 0), sigmaX=25)
    r = analyze_integrity(_png(img))
    assert r["ok"] is False
    assert "extreme_blur" in r["reasons"]


def test_integrity_undecodable_bytes():
    r = analyze_integrity(b"not an image at all")
    assert r["ok"] is False
    assert "undecodable" in r["reasons"]
