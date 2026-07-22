import image_platform as ip

def test_variant_backend_defaults_banana():
    assert ip.ImageVariant.__table__.c.backend.default.arg == "banana"

def test_variant_to_dict_emits_backend():
    v = ip.ImageVariant(node_id=1, variant_index=1, image_path="p.png", backend="chatgpt")
    assert v.to_dict()["backend"] == "chatgpt"

def test_variant_to_dict_backend_falls_back_to_banana():
    v = ip.ImageVariant(node_id=1, variant_index=1, image_path="p.png")
    assert v.to_dict()["backend"] == "banana"
