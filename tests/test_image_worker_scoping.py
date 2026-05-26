# tests/test_image_worker_scoping.py
#
# v759: verify that _worker_http_is_online is scoped to user_id.
#
# Self-contained: builds its own in-memory SQLite engine + table, so it
# does NOT require the full FastAPI app to import successfully.
#
# Strategy: use ast.get_source_segment to extract _worker_http_is_online
# source, then exec it in an isolated namespace against the in-memory
# session.  This sidesteps the pre-existing FastAPI version-mismatch that
# prevents "import image_platform" from succeeding in the local env.

import ast
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import Column, String, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ---------------------------------------------------------------------------
# Minimal model replica (mirrors ImageWorkerHeartbeat exactly)
# ---------------------------------------------------------------------------
_Base = declarative_base()


class _ImageWorkerHeartbeat(_Base):
    __tablename__ = "image_worker_heartbeats"
    worker_id = Column(String(100), primary_key=True)
    user_id = Column(String, nullable=True, index=True)
    last_heartbeat_at = Column(DateTime, default=datetime.utcnow)


def _make_session() -> Session:
    eng = create_engine("sqlite:///:memory:")
    _Base.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


# ---------------------------------------------------------------------------
# Extract _worker_http_is_online from image_platform.py via AST
# ---------------------------------------------------------------------------
_PLATFORM_PATH = Path(__file__).parent.parent / "image_platform.py"


def _load_function_source(name: str) -> str:
    src = _PLATFORM_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node)
    raise RuntimeError(f"{name} not found in image_platform.py")


def _build_online_fn(heartbeat_cls, stale_seconds: int = 15):
    """Exec _worker_http_is_online in isolation, wired to our test model."""
    fn_src = _load_function_source("_worker_http_is_online")
    # Dedent so exec sees clean top-level code
    fn_src = textwrap.dedent(fn_src)

    ns: dict = {
        "datetime": datetime,
        "timedelta": timedelta,
        "Session": Session,
        "Optional": __import__("typing").Optional,
        "ImageWorkerHeartbeat": heartbeat_cls,
        "WORKER_HEARTBEAT_STALE_SECONDS": stale_seconds,
    }
    exec(fn_src, ns)  # noqa: S102
    return ns["_worker_http_is_online"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_online_scoped_to_user():
    """Fresh heartbeat owned by userA -> online for userA, offline for userB."""
    db = _make_session()
    fn = _build_online_fn(_ImageWorkerHeartbeat)

    db.add(_ImageWorkerHeartbeat(
        worker_id="image-worker-hostA",
        user_id="userA",
        last_heartbeat_at=datetime.utcnow(),
    ))
    db.commit()

    assert fn(db, user_id="userA") is True,  "userA should be online"
    assert fn(db, user_id="userB") is False, "userB should be offline"


def test_online_no_user_filter_sees_any():
    """When user_id=None (legacy / unscoped), any fresh row counts."""
    db = _make_session()
    fn = _build_online_fn(_ImageWorkerHeartbeat)

    db.add(_ImageWorkerHeartbeat(
        worker_id="image-worker-hostA",
        user_id="userA",
        last_heartbeat_at=datetime.utcnow(),
    ))
    db.commit()

    assert fn(db) is True, "unscoped call should still return True"


def test_stale_row_is_offline():
    """A row older than the stale window is not online for anyone."""
    db = _make_session()
    fn = _build_online_fn(_ImageWorkerHeartbeat, stale_seconds=15)

    db.add(_ImageWorkerHeartbeat(
        worker_id="image-worker-hostA",
        user_id="userA",
        last_heartbeat_at=datetime.utcnow() - timedelta(seconds=30),
    ))
    db.commit()

    assert fn(db, user_id="userA") is False, "stale row should be offline"


def test_multiple_users_isolated():
    """Two workers, two users — each user only sees their own worker."""
    db = _make_session()
    fn = _build_online_fn(_ImageWorkerHeartbeat)

    db.add(_ImageWorkerHeartbeat(
        worker_id="worker-A",
        user_id="alice",
        last_heartbeat_at=datetime.utcnow(),
    ))
    db.add(_ImageWorkerHeartbeat(
        worker_id="worker-B",
        user_id="bob",
        last_heartbeat_at=datetime.utcnow(),
    ))
    db.commit()

    assert fn(db, user_id="alice") is True
    assert fn(db, user_id="bob") is True
    assert fn(db, user_id="carol") is False
