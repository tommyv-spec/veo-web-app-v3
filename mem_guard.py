"""v865 — cgroup-aware memory reporting.

WHY THIS EXISTS
---------------
Render runs this app in a container with a hard 2GB limit ("plan: standard").
The kernel OOM-kills us against the *cgroup* limit, but `/proc/meminfo` inside a
container reports the **HOST's** memory — a number with no relationship to the
limit we die at. v864 built its support-track guard on `MemAvailable` and so
read a huge free-memory figure on a box that was minutes from an OOM kill; the
guard could never fire. Nothing else in this codebase reads cgroup files at all.

Everything here reads the cgroup first (v2, then v1) and only falls back to
/proc/meminfo when there is genuinely no cgroup limit (bare metal, local dev).

PAGE CACHE
----------
cgroup v2's `memory.current` INCLUDES page cache. An export writes several
hundred MB of mp4, so `memory.current` climbs toward the limit even though the
kernel can drop that cache instantly under pressure. Treating it as "used"
would make every guard refuse to run. We therefore subtract the reclaimable
part (`inactive_file` + `slab_reclaimable`) to get a realistic headroom figure.
"""

from pathlib import Path

# Overridable for tests.
CGROUP_V2_ROOT = Path("/sys/fs/cgroup")
CGROUP_V1_MEM_ROOT = Path("/sys/fs/cgroup/memory")
PROC_MEMINFO = Path("/proc/meminfo")
PROC_SELF_STATUS = Path("/proc/self/status")

# cgroup v1 writes a huge sentinel instead of "max" when unlimited.
_V1_UNLIMITED = 0x7FFFFFFFFFFFF000


def _read_text(path):
    try:
        return path.read_text()
    except Exception:
        return None


def _read_int(path):
    raw = _read_text(path)
    if raw is None:
        return None
    raw = raw.strip()
    if raw == "max":          # cgroup v2 unlimited
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _parse_keyed(raw):
    """Parse a 'key value' file (memory.stat) into {key: int}."""
    out = {}
    for line in (raw or "").splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return out


def limit_bytes():
    """Container memory limit, or None when unlimited/unavailable."""
    v2 = _read_int(CGROUP_V2_ROOT / "memory.max")
    if v2:
        return v2
    v1 = _read_int(CGROUP_V1_MEM_ROOT / "memory.limit_in_bytes")
    if v1 and v1 < _V1_UNLIMITED:
        return v1
    return None


def usage_bytes():
    """Current charged usage (INCLUDES page cache), or None."""
    v2 = _read_int(CGROUP_V2_ROOT / "memory.current")
    if v2 is not None:
        return v2
    return _read_int(CGROUP_V1_MEM_ROOT / "memory.usage_in_bytes")


def reclaimable_bytes():
    """Page cache + reclaimable slab the kernel can drop before OOM-killing."""
    stat = _parse_keyed(_read_text(CGROUP_V2_ROOT / "memory.stat"))
    if not stat:
        stat = _parse_keyed(_read_text(CGROUP_V1_MEM_ROOT / "memory.stat"))
    if not stat:
        return 0
    total = 0
    for key in ("inactive_file", "slab_reclaimable"):
        total += stat.get(key, 0) or 0
    return total


def rss_bytes():
    """This process's resident set size."""
    raw = _read_text(PROC_SELF_STATUS)
    for line in (raw or "").splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1]) * 1024
            except (ValueError, IndexError):
                return None
    return None


def _meminfo_available_bytes():
    raw = _read_text(PROC_MEMINFO)
    for line in (raw or "").splitlines():
        if line.startswith("MemAvailable:"):
            try:
                return int(line.split()[1]) * 1024
            except (ValueError, IndexError):
                return None
    return None


def snapshot():
    """Memory picture in MB.

    Keys: source, limit_mb, used_mb, avail_mb, rss_mb.
    `source` is 'cgroup' when the container limit is known (the number we are
    actually killed against) and 'meminfo-host' when we had to fall back to the
    host figure — which is NOT a container headroom number and must not be
    trusted for guard decisions.
    """
    def _mb(v):
        return None if v is None else int(v // (1024 * 1024))

    limit = limit_bytes()
    rss = rss_bytes()

    if limit:
        used = usage_bytes() or 0
        effective_used = max(0, used - reclaimable_bytes())
        return {
            "source": "cgroup",
            "limit_mb": _mb(limit),
            "used_mb": _mb(effective_used),
            "avail_mb": _mb(max(0, limit - effective_used)),
            "rss_mb": _mb(rss),
        }

    return {
        "source": "meminfo-host",
        "limit_mb": None,
        "used_mb": None,
        "avail_mb": _mb(_meminfo_available_bytes()),
        "rss_mb": _mb(rss),
    }


def format_line(tag):
    s = snapshot()
    return (
        f"[Mem/v865] {tag}: avail={s['avail_mb']}MB used={s['used_mb']}MB "
        f"limit={s['limit_mb']}MB rss={s['rss_mb']}MB src={s['source']}"
    )


def log(tag):
    """Print a uniform memory line. Never raises."""
    try:
        print(format_line(tag), flush=True)
    except Exception:
        pass


# === v866 — continuous sampler ==============================================
# WHY A THREAD, NOT AN ASYNCIO TASK: the export runs sync ffmpeg/whisper work
# via asyncio.to_thread, but plenty of other code blocks the event loop
# outright. An asyncio sampler would simply not be scheduled during exactly the
# window we need to observe, and the trace would go dark right before the OOM —
# which is what happened on 2026-07-23: the last line before the instance
# restarted was a [Support] placement, leaving no idea what allocated next.
#
# The sampler also records a PHASE label so a spike can be attributed to the
# code that caused it, and it prints the peak seen so far, so even the LAST
# line before an OOM kill carries the high-water mark.

_PHASE = "idle"
_PEAK_USED_MB = 0
_sampler_started = False


def set_phase(phase):
    """Label what the process is doing; appears in every sample line."""
    global _PHASE
    _PHASE = str(phase)


def peak_used_mb():
    return _PEAK_USED_MB


def _sample_once(force=False, tag="sample"):
    global _PEAK_USED_MB
    s = snapshot()
    used = s.get("used_mb")
    if used is not None and used > _PEAK_USED_MB:
        _PEAK_USED_MB = used
    pct = None
    if used is not None and s.get("limit_mb"):
        pct = int(100 * used / s["limit_mb"])
    # Only speak up when it matters: crossing 60% of the limit, or on force.
    if force or (pct is not None and pct >= 60):
        level = "WARN" if (pct is not None and pct >= 80) else "info"
        print(
            f"[Mem/v866] {tag} {level} phase={_PHASE} used={used}MB "
            f"({pct}% of {s.get('limit_mb')}MB) avail={s.get('avail_mb')}MB "
            f"rss={s.get('rss_mb')}MB peak={_PEAK_USED_MB}MB",
            flush=True,
        )
    return s


def start_sampler(interval=3.0):
    """Start the background memory sampler exactly once. Never raises."""
    global _sampler_started
    if _sampler_started:
        return
    _sampler_started = True
    try:
        import threading

        def _loop():
            while True:
                try:
                    _sample_once()
                except Exception:
                    pass
                try:
                    import time
                    time.sleep(interval)
                except Exception:
                    return

        t = threading.Thread(target=_loop, name="mem-sampler", daemon=True)
        t.start()
        log("sampler started")
    except Exception:
        pass
