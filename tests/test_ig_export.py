"""v878 / v878.1 — Instagram CSV export: window math, row shape, build title.

The window is half the feature: "last month" is a rolling 30 days, but a named
month is a CALENDAR month (asking for 2026-06 must not hand back late May), and
December must roll the year.

The other half is identifying the reel. video_title is the BUILD title — the
image batch this job was promoted from (v780.1's lookup) — and video_name falls
back job-line → caption → shortcode. A reel with no posting date cannot be placed
in any window, so it is skipped and counted rather than silently dropped.

Real in-memory SQLite + real models — the filters are the thing under test.
"""
import csv
import io
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main
from image_platform import ImageJobBatch
from models import Base, User, Job, Clip, InstagramAccount, InstagramVideo

_JOB = "c9f0e6c9-2222-4000-8000-000000000078"
_TITLE = "nuri-korella-ed-penthouse-morph-selling-v3"


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
    batch = ImageJobBatch(id="b-0001", user_id="u1", name=_TITLE, promoted_video_job_id=_JOB)
    acc = InstagramAccount(id=1, user_id="u1", handle="korella", api_key_encrypted="x")
    rows = [
        # matched → title from the promoted batch, name from the job's first clip
        InstagramVideo(id=1, account_id=1, shortcode="AAA", url="https://instagram.com/reel/AAA",
                       views=1234, posted_at=now - timedelta(days=2), matched_job_id=_JOB),
        # unmatched, has caption → no title, name from the caption's first line
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
    db.add_all([user, job, batch, acc, *clips, *rows])
    db.commit()
    return db, user


def test_last_week_window_holds_only_seven_days():
    start, end, label = main._ig_export_window("last_week")
    assert label == "last-7d"
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

    assert [r[3] for r in rows] == [1, 2]           # newest first, 20d + 200d excluded
    assert rows[0][1] == "he was sixty but his soldier never stood down"  # clip_index 0
    assert rows[1][1] == "puffy face fix"           # caption first line
    assert rows[0][5] == 1234                       # views
    assert undated == 1                             # the dateless reel is reported


def test_matched_reel_carries_the_build_title_and_unmatched_carries_none():
    """v878.1 — the title comes from the image batch promoted to the matched job."""
    db, _ = _seed()
    start, end, _ = main._ig_export_window("last_week")
    rows, _ = main._ig_export_rows(db, 1, start, end)

    assert rows[0][0] == _TITLE   # reel AAA is matched to the job that batch built
    assert rows[1][0] == ""       # reel BBB has no match → no title, not a guess


def test_title_lookup_failure_does_not_kill_the_export(monkeypatch):
    """A missing image_job_batches table costs a blank column, not the CSV."""
    db, _ = _seed()
    real_query = db.query

    def _query(*args, **kwargs):
        if args and args[0] is ImageJobBatch.promoted_video_job_id:
            raise RuntimeError("no such table: image_job_batches")
        return real_query(*args, **kwargs)

    monkeypatch.setattr(db, "query", _query)
    start, end, _ = main._ig_export_window("last_week")
    rows, _ = main._ig_export_rows(db, 1, start, end)

    assert [r[3] for r in rows] == [1, 2]   # rows still produced
    assert rows[0][0] == ""                 # title column blank, nothing raised


def test_thirty_day_window_reaches_further_back_and_shortcode_is_the_last_resort():
    db, _ = _seed()
    start, end, _ = main._ig_export_window("last_month")
    rows, _ = main._ig_export_rows(db, 1, start, end)

    assert [r[3] for r in rows] == [1, 2, 3]
    assert rows[2][1] == "CCC"              # no job, no caption → shortcode


def test_endpoint_returns_a_parseable_csv_with_the_six_columns():
    db, user = _seed()
    resp = main.export_instagram_videos(1, range="last_month", month=None, db=db, current_user=user)

    assert resp.media_type.startswith("text/csv")
    assert 'filename="ig-korella-last-30d.csv"' in resp.headers["content-disposition"]
    assert resp.headers["x-ig-export-rows"] == "3"
    assert resp.headers["x-ig-export-undated-skipped"] == "1"

    text = resp.body.decode("utf-8-sig")
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[0] == ["video_title", "video_name", "video_url", "video_id", "posted_at", "views"]
    assert len(parsed) == 4
    assert parsed[1][0] == _TITLE
    assert parsed[1][2] == "https://instagram.com/reel/AAA"
    assert parsed[1][5] == "1234"
