import image_platform as ip


def test_apply_chatgpt_completion_sets_cg_ready():
    class N:
        status="generating"; cg_status="generating"
        claimed_by_worker="w"; cg_claimed_by="w"; claimed_at=1; cg_claimed_at=1
        chosen_variant_id=None; error_message=None
    n = N()
    ip._apply_worker_status(n, "chatgpt", "completed", has_variants=True, error=None)
    assert n.cg_status == "ready"
    assert n.status == "generating"          # banana lane untouched
    assert n.cg_claimed_by is None


def test_apply_chatgpt_failure_sets_cg_failed():
    class N:
        status="ready"; cg_status="generating"; cg_claimed_by="w"; cg_claimed_at=1
        chosen_variant_id=5; error_message=None; claimed_by_worker=None; claimed_at=None
    n = N()
    ip._apply_worker_status(n, "chatgpt", "failed", has_variants=False, error="boom")
    assert n.cg_status == "failed"
    assert n.status == "ready"               # banana result preserved


def test_apply_chatgpt_completion_no_variants_fails_cg_only():
    class N:
        status="generating"; cg_status="generating"; cg_claimed_by="w"; cg_claimed_at=1
        claimed_by_worker="w"; claimed_at=1; chosen_variant_id=None; error_message=None
    n = N()
    ip._apply_worker_status(n, "chatgpt", "completed", has_variants=False, error=None)
    assert n.cg_status == "failed"
    assert n.status == "generating"          # banana untouched even on cg failure


def test_apply_banana_completion_sets_status_ready():
    class N:
        status="generating"; cg_status="queued"; claimed_by_worker="w"; claimed_at=1
        chosen_variant_id=None; error_message=None; cg_claimed_by=None; cg_claimed_at=None
    n = N()
    ip._apply_worker_status(n, "banana", "completed", has_variants=True, error=None)
    assert n.status == "ready"
    assert n.cg_status == "queued"           # cg lane still pending its own worker


def test_apply_banana_completion_no_variants_fails():
    class N:
        status="generating"; cg_status=None; claimed_by_worker="w"; claimed_at=1
        chosen_variant_id=None; error_message=None; cg_claimed_by=None; cg_claimed_at=None
    n = N()
    ip._apply_worker_status(n, "banana", "completed", has_variants=False, error=None)
    assert n.status == "failed"
    assert "no variants" in (n.error_message or "").lower()


def test_apply_unknown_status_raises():
    class N:
        status="generating"; cg_status=None; claimed_by_worker=None; claimed_at=None
        cg_claimed_by=None; cg_claimed_at=None; chosen_variant_id=None; error_message=None
    import pytest
    with pytest.raises(ValueError):
        ip._apply_worker_status(N(), "banana", "weird", has_variants=True, error=None)
