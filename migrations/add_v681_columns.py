"""v681 — multi-character cast model + text-card scene type.

Adds:
- image_nodes.cast_json TEXT
- image_scene_assignments.cast_json TEXT
- image_scene_assignments.scene_type VARCHAR(20)
- image_scene_assignments.caption TEXT
- image_scene_assignments.bg_color VARCHAR(20)
- image_scene_assignments.duration_s DOUBLE PRECISION
- image_scene_assignments.image_node_id → DROP NOT NULL (text_card scenes)
- clips.caption TEXT
- clips.scene_type VARCHAR(20)
- clips.bg_color VARCHAR(20)

All new columns are nullable; NULL = legacy v509 prompt-scan + Veo render
path. Idempotent — re-runs print '=column.already-present' instead of
failing. Mirror of the entries added to image_platform.py's
_apply_pending_migrations sqlite_migrations + postgres_migrations lists.
"""
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, IntegrityError, ProgrammingError


_STATEMENTS = [
    ("image_nodes", "cast_json",
     "ALTER TABLE image_nodes ADD COLUMN cast_json TEXT"),
    ("image_scene_assignments", "cast_json",
     "ALTER TABLE image_scene_assignments ADD COLUMN cast_json TEXT"),
    ("image_scene_assignments", "scene_type",
     "ALTER TABLE image_scene_assignments ADD COLUMN scene_type VARCHAR(20)"),
    ("image_scene_assignments", "caption",
     "ALTER TABLE image_scene_assignments ADD COLUMN caption TEXT"),
    ("image_scene_assignments", "bg_color",
     "ALTER TABLE image_scene_assignments ADD COLUMN bg_color VARCHAR(20)"),
    ("image_scene_assignments", "duration_s",
     "ALTER TABLE image_scene_assignments ADD COLUMN duration_s FLOAT"),
    ("image_scene_assignments", "image_node_id_nullable",
     "ALTER TABLE image_scene_assignments ALTER COLUMN image_node_id DROP NOT NULL"),
    ("clips", "caption",
     "ALTER TABLE clips ADD COLUMN caption TEXT"),
    ("clips", "scene_type",
     "ALTER TABLE clips ADD COLUMN scene_type VARCHAR(20)"),
    ("clips", "bg_color",
     "ALTER TABLE clips ADD COLUMN bg_color VARCHAR(20)"),
]


def upgrade(engine):
    """Apply v681 schema additions. Idempotent — re-runs are safe."""
    with engine.begin() as conn:
        for table, col, stmt in _STATEMENTS:
            try:
                conn.execute(text(stmt))
                print(f"[v681-migration] +{table}.{col}")
            except (OperationalError, IntegrityError, ProgrammingError) as e:
                msg = str(e).lower()
                if (
                    "duplicate" in msg
                    or "already exists" in msg
                    or "no such column" in msg
                    or "near \"alter\"" in msg  # SQLite refuses ALTER COLUMN; tolerated.
                ):
                    print(f"[v681-migration] ={table}.{col} (already present or N/A on this engine)")
                else:
                    print(f"[v681-migration] !{table}.{col} {type(e).__name__}: {str(e)[:160]}")


if __name__ == "__main__":
    from models import engine
    upgrade(engine)
    print("[v681-migration] done.")
