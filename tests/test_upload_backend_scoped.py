import image_platform as ip


def test_same_backend_filter_only_deletes_matching():
    class V:
        def __init__(self, b): self.backend = b; self.source = "ai"
    variants = [V("banana"), V("banana"), V("chatgpt")]
    # replacing the chatgpt backend -> only the chatgpt variant is replaceable
    keep = [v for v in variants if not ip._variant_replaceable(v, "chatgpt")]
    assert len(keep) == 2 and all(v.backend == "banana" for v in keep)


def test_manual_never_replaced():
    class V:
        def __init__(self, b, s): self.backend = b; self.source = s
    assert ip._variant_replaceable(V("banana", "manual"), "banana") is False


def test_same_backend_ai_is_replaceable():
    class V:
        def __init__(self, b, s): self.backend = b; self.source = s
    assert ip._variant_replaceable(V("chatgpt", "ai"), "chatgpt") is True


def test_other_backend_not_replaceable():
    class V:
        def __init__(self, b, s): self.backend = b; self.source = s
    assert ip._variant_replaceable(V("banana", "ai"), "chatgpt") is False
