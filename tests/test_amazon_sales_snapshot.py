"""The Performance tab's snapshot lives in the DATABASE, not on the container.

WHY THIS FILE EXISTS. The snapshot was a JSON file on disk. It went to /tmp,
which says on the tin that it does not survive; that was "fixed" by writing to
/app/data, which render.yaml declares as a mounted disk. The live service has no
disk -- `GET /services/<id>/disk` answers 404 -- so /app/data is ephemeral too.
Every deploy emptied the Performance tab, and nothing caught it, because the
push's own answer was honest the whole time: it DID store the file.

The log line naming the path looked like proof of persistence and was not. Only
a read AFTER a restart can prove that, and these tests are the cheap stand-in:
they assert the bytes go somewhere the container does not own.

Run: python -m pytest code/tests/test_amazon_sales_snapshot.py -q
"""
import asyncio
import json
import sys
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import image_platform  # noqa: F401 -- registers image_nodes, which other FKs point at
import main
from models import Base, User, AmazonSalesSnapshot


@pytest.fixture
def db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine)()
    s.add(User(id="u1", email="operator@example.com"))
    s.add(User(id="u2", email="someone.else@example.com"))
    s.commit()
    yield s
    s.close()


def _payload(videos=1, generated="2026-09-17T00:00:00+00:00"):
    return {"videos": [{"video": f"v{i}"} for i in range(videos)],
            "period": {"from": "2026-09-08", "to": "2026-09-15"},
            "generated_at": generated}


def _store(db, user_id, payload):
    return asyncio.run(main.receive_amazon_sales_report(
        payload=payload, user_id=user_id, db=db))


class _User:
    def __init__(self, uid):
        self.id = uid


def _read(db, user_id):
    return asyncio.run(main.read_amazon_sales_report(
        current_user=_User(user_id), db=db))


def _read_as_worker(db, user_id, full=0):
    return asyncio.run(main.read_amazon_sales_report_for_worker(
        full=full, user_id=user_id, db=db))


def _fb_payload():
    """A snapshot shaped the way the push builds one after the Facebook work."""
    return {
        "generated_at": "2026-09-17T02:00:00+00:00",
        "period": {"from": "2026-09-08", "to": "2026-09-15"},
        "videos": [
            {"video": "both.mp4", "views": 4000, "facebook_views": 1000,
             "total_views": 5000},
            {"video": "ig-only.mp4", "views": 300, "facebook_views": "Unknown",
             "total_views": 300},
        ],
        "facebook": {"by_vendor": {
            "blotato": {"posts_read": 43, "posts_matched": 43,
                        "views_matched": 189900, "views_unmatched": 0}}},
    }


def test_the_worker_can_read_the_stored_snapshot_back(db):
    """Without this route nothing outside a browser could check what the platform
    actually holds, and "the push said stored" is not that check -- rev 1021."""
    _store(db, "u1", _fb_payload())
    got = _read_as_worker(db, "u1")
    assert got["stored"] is True
    assert got["videos"] == 2
    assert got["videos_with_views"] == 2
    assert got["videos_with_facebook_views"] == 1
    assert got["facebook"]["by_vendor"]["blotato"]["views_matched"] == 189900
    assert "report" not in got          # the counters only, unless asked


def test_full_returns_the_whole_stored_payload(db):
    _store(db, "u1", _fb_payload())
    got = _read_as_worker(db, "u1", full=1)
    assert [v["video"] for v in got["report"]["videos"]] == ["both.mp4", "ig-only.mp4"]


def test_the_worker_route_says_which_kind_of_nothing_it_found(db):
    """"Nothing pushed" and "the stored bytes will not parse" send a reader to two
    different places, and _amazon_sales_latest answers None to both."""
    empty = _read_as_worker(db, "u1")
    assert empty["stored"] is False and "pushed" in empty["reason"]

    db.add(AmazonSalesSnapshot(user_id="u1", payload="{not json", videos=1))
    db.commit()
    corrupt = _read_as_worker(db, "u1")
    assert corrupt["stored"] is False and "parse" in corrupt["reason"]


def test_the_worker_route_never_reads_another_users_snapshot(db):
    _store(db, "u1", _fb_payload())
    assert _read_as_worker(db, "u2")["stored"] is False
    assert _read_as_worker(db, "u1")["stored"] is True


def test_an_unknown_facebook_cell_is_never_counted_as_a_number(db):
    """Unknown, never 0 -- the whole rule this panel exists to keep."""
    _store(db, "u1", {"generated_at": "x", "videos": [
        {"video": "a", "views": "Unknown", "facebook_views": "Unknown"}]})
    got = _read_as_worker(db, "u1")
    assert got["videos"] == 1
    assert got["videos_with_views"] == 0
    assert got["videos_with_facebook_views"] == 0


def test_a_pushed_snapshot_comes_back_out(db):
    answer = _store(db, "u1", _payload(videos=3))
    assert answer["stored"] is True and answer["videos"] == 3
    got = _read(db, "u1")
    assert got["stored"] is True
    assert len(got["report"]["videos"]) == 3


def test_nothing_pushed_says_so_rather_than_failing(db):
    """The tab has to tell "no data yet" from "the endpoint is broken"."""
    got = _read(db, "u1")
    assert got["stored"] is False
    assert "pushed" in got["reason"]


def test_the_newest_push_is_the_one_that_is_read(db):
    _store(db, "u1", _payload(videos=1, generated="2026-09-16T00:00:00+00:00"))
    _store(db, "u1", _payload(videos=9, generated="2026-09-17T00:00:00+00:00"))
    got = _read(db, "u1")
    assert len(got["report"]["videos"]) == 9


def test_one_users_numbers_never_reach_another(db):
    """These are somebody's own Amazon earnings. A shared row would show whoever
    logged in first the numbers of whoever pushed last."""
    _store(db, "u1", _payload(videos=4))
    assert _read(db, "u2")["stored"] is False
    assert len(_read(db, "u1")["report"]["videos"]) == 4


def test_older_snapshots_are_kept_but_bounded(db):
    """Kept, because when a push lands wrong the only way to see WHAT changed is
    to still hold what it replaced. Bounded, because a daily push must not grow
    the table for ever."""
    for i in range(main._AMAZON_SALES_KEEP + 3):
        _store(db, "u1", _payload(videos=i + 1))
    rows = db.query(AmazonSalesSnapshot).filter_by(user_id="u1").all()
    assert len(rows) == main._AMAZON_SALES_KEEP
    # The pruning kept the NEWEST, not the first ones it found.
    assert len(_read(db, "u1")["report"]["videos"]) == main._AMAZON_SALES_KEEP + 3


def test_a_payload_that_is_not_a_report_is_refused(db):
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        _store(db, "u1", {"not": "a report"})
    assert exc.value.status_code == 400
    assert _read(db, "u1")["stored"] is False


def test_an_unreadable_row_reads_as_no_report_not_as_a_crash(db):
    """A row we cannot parse must not take the tab down with it."""
    db.add(AmazonSalesSnapshot(user_id="u1", payload="{not json", videos=1))
    db.commit()
    assert _read(db, "u1")["stored"] is False


def test_the_export_columns_come_from_the_same_row(db):
    """The CSV export and the Performance tab must never disagree about what
    was pushed -- they read one row, not two copies."""
    payload = _payload(videos=0)
    payload["videos"] = [{"video": "v0",
                          "instagram_url": "https://instagram.com/reel/AAA/",
                          "period": {"clicks": {"value": 4}}}]
    _store(db, "u1", payload)
    by_url = main._ig_export_sales(db, "u1")
    assert "https://instagram.com/reel/AAA" in by_url


def test_storing_writes_no_file_anywhere(db, tmp_path, monkeypatch):
    """The whole point. If this ever starts touching the filesystem again, the
    next deploy empties the tab and the push still answers "stored"."""
    monkeypatch.chdir(tmp_path)
    _store(db, "u1", _payload(videos=2))
    assert list(tmp_path.rglob("*.json")) == []
    assert json.loads(db.query(AmazonSalesSnapshot)
                      .filter_by(user_id="u1").one().payload)["videos"]
