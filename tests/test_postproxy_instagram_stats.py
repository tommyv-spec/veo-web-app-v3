"""Postproxy is the connected-account stats source; Hiker remains discovery only."""
import asyncio
import os
import sys

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import image_platform  # noqa: F401 -- registers tables referenced by foreign keys
import main
from models import Base, InstagramAccount, InstagramVideo, User


@pytest.fixture
def db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add_all([User(id="u1", email="one@example.com"),
                     User(id="u2", email="two@example.com")])
    session.commit()
    yield session
    session.close()


def account(db, user, handle):
    row = InstagramAccount(user_id=user, handle=handle,
                           api_key_encrypted="legacy-discovery-key")
    db.add(row)
    db.commit()
    return row


def reel(db, acc, shortcode, views=0, likes=0, comments=0):
    row = InstagramVideo(account_id=acc.id, shortcode=shortcode,
                         url=f"https://instagram.com/reel/{shortcode}/",
                         views=views, likes=likes, comments=comments)
    db.add(row)
    db.commit()
    return row


def push(db, user, rows, source="postproxy"):
    return asyncio.run(main.receive_postproxy_instagram_stats(
        payload={"source": source, "rows": rows}, user_id=user, db=db))


def test_postproxy_updates_the_existing_connected_account_row(db):
    acc = account(db, "u1", "martha_health_style")
    video = reel(db, acc, "ABC", views=10, likes=1, comments=0)

    answer = push(db, "u1", [{"shortcode": "ABC", "views": 4321,
                              "likes": 55, "comments": 7,
                              "reach": 3000, "captured_at": "2026-09-17"}])

    db.refresh(video)
    assert answer == {"source": "postproxy", "received": 1, "usable": 1,
                      "matched": 1, "updated": 1, "readback": 1}
    assert (video.views, video.likes, video.comments) == (4321, 55, 7)


def test_a_worker_token_never_updates_another_users_reel(db):
    own = reel(db, account(db, "u1", "one"), "SAME", views=1)
    other = reel(db, account(db, "u2", "two"), "SAME", views=99)

    answer = push(db, "u1", [{"shortcode": "SAME", "views": 500}])

    db.refresh(own)
    db.refresh(other)
    assert answer["matched"] == 1
    assert own.views == 500
    assert other.views == 99


def test_sparse_zero_never_erases_a_real_count(db):
    video = reel(db, account(db, "u1", "one"), "KEEP",
                 views=800, likes=20, comments=3)
    push(db, "u1", [{"shortcode": "KEEP", "views": 0,
                     "likes": 0, "comments": 0}])
    db.refresh(video)
    assert (video.views, video.likes, video.comments) == (800, 20, 3)


def test_unknown_reels_are_not_created_by_the_stats_feed(db):
    account(db, "u1", "one")
    answer = push(db, "u1", [{"shortcode": "NOT-STORED", "views": 900}])
    assert answer["matched"] == 0
    assert db.query(InstagramVideo).count() == 0


def test_only_postproxy_payloads_are_accepted(db):
    with pytest.raises(HTTPException) as exc:
        push(db, "u1", [], source="something-else")
    assert exc.value.status_code == 400
