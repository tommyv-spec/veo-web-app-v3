"""v878 — Instagram CSV export: window math + row shape.

The window is the whole feature: "last month" is a rolling 30 days, but a named
month is a CALENDAR month (asking for 2026-06 must not hand back late May), and
December must roll the year. Row naming falls back job-line → caption → shortcode,
and a reel with no posting date cannot be placed in any window so it is skipped
and counted.

Real in-memory SQLite + real models — the filters are the thing under test.
"""
import csv
import io
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main
from models import Base, User, Job, Clip, InstagramAccount, InstagramVideo

_JOB = "c9f0e6c9-2222-4000-8000-000000000078"


def _seed():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    now = datetime.utcnow()

    user = User(id="u1", email="operator@example.com")
    job = Job(id=_JOB, user_id="u1", config_json="{}", dialogue_json="[]",
              images_dir="/img", output_dir="/out", status="completed")
    # clip_index 1 first on purpose: the name must come from index 0.
    clips = [
        Clip(job_id=_JOB, clip_index=1, dialogue_id=2, dialogue_text="second line"),
        Clip(job_id=_JOB, clip_index=0, dialogue_id=1, dialogue_text="he was sixty but his soldier never stood down"),
    ]
    acc = InstagramAccount(id=1, user_id="u1", handle="korella", api_key_encrypted="x")
    rows = [
        # matched → name from the job's first clip line
        InstagramVideo(id=1, account_id=1, shortcode="AAA", url="https://instagram.com/reel/AAA",
                       views=1234, posted_at=now - timedelta(days=2), matched_job_id=_JOB),
        # unmatched, has caption → name from the caption's first line
        InstagramVideo(id=2, account_id=1, shortcode="BBB", url="https://instagram.com/reel/BBB",
                       views=99, posted_at=now - timedelta(days=3), caption="puffy face fix\nsecond caption line"),
        # inside 30d, outside 7d
        InstagramVideo(id=3, account_id=1, shortcode="CCC", url="https://instagram.com/reel/CCC",
                       views=7, posted_at=now - timedelta(days=20)),
        # older than every rolling window
        InstagramVideo(id=4, account_id=1, shortcode="DDD", url="https://instagram.com/reel/DDD",
                       views=5, posted_at=now - timedelta(days=200)),
        # never synced a date → unplaceable
        InstagramVideo(id=5, account_id=1, shortcode="EEE", url="https://instagram.com/reel/EEE", views=3),
    ]
    db.add_all([user, job, acc, *clips, *rows])
    db.commit()
    return db, user


def test_last_week_window_holds_only_seven_days():
    start, end, label = main._ig_export_window("last_week")
    assert label == "last-7d"
    assert 6.9 < (end - start).days + 1 <= 8
    assert (end - start) == timedelta(days=7)


def test_named_month_is_a_calendar_month_not_thirty_days():
    start, end, label = main._ig_export_window("month", "2026-06")
    assert (start, end) == (datetime(2026, 6, 1), datetime(2026, 7, 1))
    assert label == "2026-06"


def test_december_rolls_the_year():
    start, end, _ = main._ig_export_window("month", "2025-12")
    assert (start, end) == (datetime(2025, 12, 1), datetime(2026, 1, 1))


def test_bad_range_and_missing_month_are_rejected():
    with pytest.raises(Exception):
        main._ig_export_window("last_decade")
    with pytest.raises(Exception):
        main._ig_export_window("month")
    with pytest.raises(Exception):
        main._ig_export_window("month", "june")


def test_rows_are_windowed_named_and_newest_first():
    db, _ = _seed()
    start, end, _ = main._ig_export_window("last_week")
    rows, undated = main._ig_export_rows(db, 1, start, end)

    assert [r[2] for r in rows] == [1, 2]          # newest first, 20d + 200d excluded
    assert rows[0][0] == "he was sixty but his soldier never stood down"  # clip_index 0
    assert rows[1][0] == "puffy face fix"          # caption first line
    assert rows[0][4] == 1234                      # views
    assert undated == 1                            # the dateless reel is reported


def test_thirty_day_window_reaches_further_back_and_shortcode_is_the_last_resort():
    db, _ = _seed()
    start, end, _ = main._ig_export_window("last_month")
    rows, _ = main._ig_export_rows(db, 1, start, end)

    assert [r[2] for r in rows] == [1, 2, 3]
    assert rows[2][0] == "CCC"                     # no job, no caption → shortcode


def test_endpoint_returns_a_parseable_csv_with_the_five_columns():
    db, user = _seed()
    resp = main.export_instagram_videos(1, range="last_month", month=None, db=db, current_user=user)

    assert resp.media_type.startswith("text/csv")
    assert 'filename="ig-korella-last-30d.csv"' in resp.headers["content-disposition"]
    assert resp.headers["x-ig-export-rows"] == "3"
    assert resp.headers["x-ig-export-undated-skipped"] == "1"

    text = resp.body.decode("utf-8-sig")
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[0] == ["video_name", "video_url", "video_id", "posted_at", "views"]
    assert len(parsed) == 4
    assert parsed[1][1] == "https://instagram.com/reel/AAA"
    assert parsed[1][4] == "1234"
