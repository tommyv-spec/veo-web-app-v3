"""
Migration: Add clip_order_json column to jobs table

This column stores an ordered list of clip IDs for lineup management.
When set, export uses this order instead of ORDER BY clip_index.

Run: python -c "from migrations.add_clip_order import migrate; migrate()"
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from models import get_db


def migrate():
    """Add clip_order_json column to jobs table"""
    print("=" * 50)
    print("Migration: Add clip_order_json column")
    print("=" * 50)
    
    with get_db() as db:
        # Check if column exists
        try:
            result = db.execute(text("SELECT clip_order_json FROM jobs LIMIT 1"))
            print("✓ Column 'clip_order_json' already exists")
            return True
        except Exception:
            db.rollback()  # PostgreSQL requires rollback after failed query
        
        # Add column
        try:
            # v860 — bound the ALTER's lock wait so a rolling-deploy lock held
            # by the still-serving old instance can't hang this blocking
            # PHASE-1 startup step before the port binds. No-op on SQLite (the
            # SELECT probe above already returned on the common path where the
            # column exists; this only runs on a genuine first-time add).
            try:
                db.execute(text("SET lock_timeout = '3s'"))
            except Exception:
                db.rollback()  # SQLite / unsupported — ignore
            db.execute(text(
                "ALTER TABLE jobs ADD COLUMN clip_order_json TEXT"
            ))
            db.commit()
            print("✓ Added column 'clip_order_json' to jobs table")
            return True
        except Exception as e:
            print(f"✗ Failed to add column: {e}")
            return False


if __name__ == "__main__":
    migrate()
