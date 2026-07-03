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
