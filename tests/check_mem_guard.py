"""v865 — mem_guard cgroup parsing tests.

The bug being locked out: v864 read /proc/meminfo MemAvailable inside a
container, which reports the HOST's free memory and not the 2GB cgroup limit we
are OOM-killed against. These tests assert the cgroup limit wins whenever it
exists, and that page cache is not counted as unavailable.

Run: python tests/check_mem_guard.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mem_guard as mg  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name} {detail}")
        FAILED.append(name)


def write(root, rel, text):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return p


def setup(tmp, *, v2=None, v1=None, stat=None, meminfo=None, rss_kb=None, v1_stat=None):
    """Point mem_guard at fixture files instead of the real /sys and /proc."""
    mg.CGROUP_V2_ROOT = tmp / "cg2"
    mg.CGROUP_V1_MEM_ROOT = tmp / "cg1"
    mg.PROC_MEMINFO = tmp / "meminfo"
    mg.PROC_SELF_STATUS = tmp / "status"
    mg.CGROUP_V2_ROOT.mkdir(parents=True, exist_ok=True)
    mg.CGROUP_V1_MEM_ROOT.mkdir(parents=True, exist_ok=True)
    if v2:
        write(tmp, "cg2/memory.max", v2[0])
        write(tmp, "cg2/memory.current", v2[1])
    if stat:
        write(tmp, "cg2/memory.stat", stat)
    if v1:
        write(tmp, "cg1/memory.limit_in_bytes", v1[0])
        write(tmp, "cg1/memory.usage_in_bytes", v1[1])
    if v1_stat:
        write(tmp, "cg1/memory.stat", v1_stat)
    if meminfo:
        write(tmp, "meminfo", meminfo)
    if rss_kb is not None:
        write(tmp, "status", f"Name:\tpython\nVmRSS:\t{rss_kb} kB\n")


def main():
    import tempfile
    GB = 1024 ** 3
    host_meminfo = "MemTotal:       65000000 kB\nMemAvailable:   40000000 kB\n"

    # 1. THE REGRESSION: cgroup limit must win over a huge host MemAvailable.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp,
              v2=(str(2 * GB), str(int(1.8 * GB))),
              stat="inactive_file 0\nslab_reclaimable 0\n",
              meminfo=host_meminfo, rss_kb=900_000)
        s = mg.snapshot()
        check("cgroup v2 limit detected", s["limit_mb"] == 2048, s)
        check("source is cgroup (not host)", s["source"] == "cgroup", s)
        check("avail reflects the 2GB limit, ~205MB", 150 <= s["avail_mb"] <= 260, s)
        check("avail is NOT the 40GB host figure", s["avail_mb"] < 1000, s)
        check("rss parsed", s["rss_mb"] == 878, s)

    # 2. Page cache must not be counted as unavailable.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp,
              v2=(str(2 * GB), str(int(1.9 * GB))),
              stat=f"inactive_file {int(1.2 * GB)}\nslab_reclaimable 0\n",
              meminfo=host_meminfo, rss_kb=500_000)
        s = mg.snapshot()
        check("reclaimable page cache excluded from used",
              1200 <= s["avail_mb"] <= 1400, s)

    # 3. cgroup v1 fallback.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp,
              v1=(str(2 * GB), str(1 * GB)),
              v1_stat="inactive_file 0\nslab_reclaimable 0\n",
              meminfo=host_meminfo, rss_kb=400_000)
        s = mg.snapshot()
        check("cgroup v1 limit detected", s["limit_mb"] == 2048, s)
        check("cgroup v1 avail ~1024MB", 1000 <= s["avail_mb"] <= 1050, s)

    # 4. v1 unlimited sentinel is not a real limit.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp, v1=(str(0x7FFFFFFFFFFFF000), str(1 * GB)),
              meminfo=host_meminfo, rss_kb=400_000)
        s = mg.snapshot()
        check("v1 unlimited sentinel ignored", s["source"] == "meminfo-host", s)

    # 5. cgroup v2 "max" (unlimited) falls back and is LABELLED as host.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp, v2=("max", str(1 * GB)), meminfo=host_meminfo, rss_kb=400_000)
        s = mg.snapshot()
        check("v2 'max' -> host fallback", s["source"] == "meminfo-host", s)
        check("host fallback is labelled, not silently trusted",
              s["limit_mb"] is None, s)

    # 6. Nothing available at all — must not raise.
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        setup(tmp)
        s = mg.snapshot()
        check("no sources -> no crash", s["avail_mb"] is None, s)
        mg.log("smoke")
        check("log() never raises", True)

    print()
    if FAILED:
        print(f"FAILED ({len(FAILED)}): {', '.join(FAILED)}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
