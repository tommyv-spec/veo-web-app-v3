from auto_image_retry import parse_auto_image_retry_mode

def test_default_mode_is_batch_when_absent():
    assert parse_auto_image_retry_mode(None) == "batch"
    assert parse_auto_image_retry_mode("") == "batch"
    assert parse_auto_image_retry_mode("not json") == "batch"
    assert parse_auto_image_retry_mode('{}') == "batch"

def test_explicit_mode_is_returned():
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"next"}}') == "next"
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"off"}}') == "off"

def test_invalid_mode_falls_back_to_batch():
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"bogus"}}') == "batch"


from auto_image_retry import order_distinct_frames, pick_substitute

def test_order_distinct_frames_dedupes_preserving_clip_order():
    clips = [
        {"clip_index": 0, "start_frame": "a"},
        {"clip_index": 1, "start_frame": "b"},
        {"clip_index": 2, "start_frame": "a"},
        {"clip_index": 3, "start_frame": "c"},
    ]
    assert order_distinct_frames(clips) == ["a", "b", "c"]

def test_pick_next_returns_following_frame():
    assert pick_substitute("next", ["a", "b", "c"], original="a", tried=["a"]) == "b"

def test_pick_prev_returns_preceding_frame():
    assert pick_substitute("prev", ["a", "b", "c"], original="b", tried=["b"]) == "a"

def test_pick_next_at_end_returns_none():
    assert pick_substitute("next", ["a", "b"], original="b", tried=["b"]) is None

def test_pick_prev_at_start_returns_none():
    assert pick_substitute("prev", ["a", "b"], original="a", tried=["a"]) is None

def test_pick_batch_returns_first_untried_other():
    assert pick_substitute("batch", ["a", "b", "c"], original="a", tried=["a"]) == "b"
    assert pick_substitute("batch", ["a", "b", "c"], original="a", tried=["a", "b"]) == "c"
    assert pick_substitute("batch", ["a", "b", "c"], original="a", tried=["a", "b", "c"]) is None

def test_off_returns_none():
    assert pick_substitute("off", ["a", "b"], original="a", tried=["a"]) is None
