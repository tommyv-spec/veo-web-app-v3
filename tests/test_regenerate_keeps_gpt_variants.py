"""v910 — regenerate is BANANA-lane scoped; ChatGPT variants survive.

Before v910 `_delete_variant_files(node)` wiped every variant row's files AND
ran a bare `variant_*.png` straggler glob, so a banana regenerate destroyed the
GPT image sitting on the same node.
"""
import types

import image_platform as ip


def _fake_node(tmp_path, files):
    for name in files:
        (tmp_path / name).write_bytes(b"x")
    variants = [
        types.SimpleNamespace(image_path="nodes/1/variant_1.png", backend="banana"),
        types.SimpleNamespace(image_path="nodes/1/variant_chatgpt_1.png", backend="chatgpt"),
    ]
    return types.SimpleNamespace(id=1, variants=variants)


def test_file_filter_banana_lane():
    f = ip._file_belongs_to_backend
    assert f("variant_1.png", "banana") is True
    assert f("variant_1.w256.webp", "banana") is True
    assert f("variant_chatgpt_1.png", "banana") is False
    assert f("variant_chatgpt_1.w256.webp", "banana") is False


def test_file_filter_chatgpt_lane_and_unscoped():
    f = ip._file_belongs_to_backend
    assert f("variant_chatgpt_2.png", "chatgpt") is True
    assert f("variant_3.png", "chatgpt") is False
    # backend=None -> full wipe, everything matches
    assert f("variant_1.png", None) is True
    assert f("variant_chatgpt_1.png", None) is True


def test_scoped_delete_keeps_chatgpt_files(tmp_path, monkeypatch):
    files = [
        "variant_1.png", "variant_1.w256.webp",
        "variant_chatgpt_1.png", "variant_chatgpt_1.w256.webp",
    ]
    node = _fake_node(tmp_path, files)
    monkeypatch.setattr(ip, "node_dir", lambda _id: tmp_path)
    monkeypatch.setattr(ip, "images_root", lambda: tmp_path.parent)
    monkeypatch.setattr(ip, "_storage_delete", lambda rel: None)

    ip._delete_variant_files(node, backend="banana")

    left = sorted(p.name for p in tmp_path.iterdir())
    assert left == ["variant_chatgpt_1.png", "variant_chatgpt_1.w256.webp"]


def test_unscoped_delete_still_wipes_everything(tmp_path, monkeypatch):
    files = ["variant_1.png", "variant_chatgpt_1.png", "variant_chatgpt_1.w256.webp"]
    node = _fake_node(tmp_path, files)
    monkeypatch.setattr(ip, "node_dir", lambda _id: tmp_path)
    monkeypatch.setattr(ip, "images_root", lambda: tmp_path.parent)
    monkeypatch.setattr(ip, "_storage_delete", lambda rel: None)

    ip._delete_variant_files(node)

    assert list(tmp_path.iterdir()) == []
