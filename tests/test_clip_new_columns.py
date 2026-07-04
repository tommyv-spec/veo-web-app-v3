from models import Clip


def test_new_columns_exist():
    cols = set(Clip.__table__.columns.keys())
    assert "dialogue_text_b" in cols
    assert "rendered_prompt_variant" in cols
