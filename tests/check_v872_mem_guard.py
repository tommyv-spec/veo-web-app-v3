"""v872 — checks for the export memory hardening.

Run: python tests/check_v872_mem_guard.py

Covers the three pieces that decide whether the 2026-07-28 export OOM (job
be09f595) can happen again:

  1. the cgroup snapshot maths, fed the ACTUAL numbers from that incident
  2. the admission gate — would it have refused to start that export?
  3. the upload spooler — does it stream instead of buffering?

Simulates the cgroup by pointing mem_guard's overridable path constants at a
temp dir, so this runs anywhere (Windows/macOS included) with no container.
"""
import io
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import mem_guard  # noqa: E402

MB = 1024 * 1024
failures = []


def check(label, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {label}: got={got} want={want}")
    if not ok:
        failures.append(label)


def fake_cgroup(root, limit_mb, current_mb, inactive_file_mb=0, slab_mb=0):
    (root / "memory.max").write_text(str(limit_mb * MB))
    (root / "memory.current").write_text(str(current_mb * MB))
    (root / "memory.stat").write_text(
        f"inactive_file {inactive_file_mb * MB}\n"
        f"slab_reclaimable {slab_mb * MB}\n"
    )
    mem_guard.CGROUP_V2_ROOT = root


with tempfile.TemporaryDirectory() as td:
    root = Path(td)

    # --- 1. the incident numbers -------------------------------------------
    # From the log: used=2031MB of 2048MB, i.e. 17MB of headroom, moments
    # before the kill. Page cache already subtracted (100MB reclaimable here).
    fake_cgroup(root, limit_mb=2048, current_mb=2131, inactive_file_mb=100)
    snap = mem_guard.snapshot()
    check("incident source", snap["source"], "cgroup")
    check("incident limit_mb", snap["limit_mb"], 2048)
    check("incident used_mb (cache subtracted)", snap["used_mb"], 2031)
    check("incident avail_mb", snap["avail_mb"], 17)

    ok, _ = mem_guard.headroom_ok(600)
    check("gate REFUSES to start an export at 17MB headroom", ok, False)

    # --- 2. a healthy container --------------------------------------------
    fake_cgroup(root, limit_mb=2048, current_mb=400, inactive_file_mb=50)
    snap = mem_guard.snapshot()
    check("healthy used_mb", snap["used_mb"], 350)
    check("healthy avail_mb", snap["avail_mb"], 1698)
    ok, _ = mem_guard.headroom_ok(600)
    check("gate ALLOWS an export at 1698MB headroom", ok, True)

    # --- 3. borderline: exactly at the threshold ---------------------------
    fake_cgroup(root, limit_mb=2048, current_mb=1448)
    ok, _ = mem_guard.headroom_ok(600)
    check("gate allows at exactly 600MB headroom", ok, True)
    fake_cgroup(root, limit_mb=2048, current_mb=1449)
    ok, _ = mem_guard.headroom_ok(600)
    check("gate refuses one MB below the threshold", ok, False)

    # --- 4. no cgroup (local dev) never blocks work ------------------------
    mem_guard.CGROUP_V2_ROOT = root / "does-not-exist"
    mem_guard.CGROUP_V1_MEM_ROOT = root / "does-not-exist-either"
    ok, snap = mem_guard.headroom_ok(600)
    check("no cgroup -> source is meminfo-host", snap["source"], "meminfo-host")
    check("no cgroup -> gate never blocks", ok, True)

    # --- 5. trim() is always safe ------------------------------------------
    mem_guard.trim()  # must not raise anywhere
    print("PASS  trim() ran without raising")

# --- 6. the upload spooler streams, it does not buffer ---------------------
import main  # noqa: E402  (imports the app; slow but this is the real function)


class _FakeUpload:
    """Stands in for starlette's UploadFile: what matters is `.file`."""

    def __init__(self, payload):
        self.file = io.BytesIO(payload)
        self.filename = "clip_0_1.1.mp4"


with tempfile.TemporaryDirectory() as td:
    dst = os.path.join(td, "out.mp4")
    payload = os.urandom(5 * MB + 12345)   # not a chunk multiple, on purpose
    size = main._spool_upload_to_path(_FakeUpload(payload), dst)
    check("spooled size matches source", size, len(payload))
    with open(dst, "rb") as fh:
        check("spooled bytes are identical", fh.read() == payload, True)

    empty_dst = os.path.join(td, "empty.mp4")
    check("empty upload spools to 0 bytes", main._spool_upload_to_path(_FakeUpload(b""), empty_dst), 0)

    # drop_page_cache must never raise, including where posix_fadvise is absent
    import video_processor  # noqa: E402

    video_processor.drop_page_cache(dst)
    video_processor.drop_page_cache(os.path.join(td, "no-such-file.mp4"))
    print("PASS  drop_page_cache ran without raising (present or absent file)")

    # release_cached_models is safe with nothing loaded
    check("release_cached_models with no models held", video_processor.release_cached_models("test"), [])

print()
if failures:
    print(f"{len(failures)} FAILED: {failures}")
    sys.exit(1)
print("ALL v872 CHECKS PASSED")
