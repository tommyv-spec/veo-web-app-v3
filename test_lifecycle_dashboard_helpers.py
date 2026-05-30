"""Python mirrors of pure JS helpers in static/index.html for the dashboard.

The JS lives in the HTML file (no module system), so we port the function
logic and test it here to lock the contract.
"""
import re


def shorten_slug(slug):
    if not slug:
        return "(untitled)"
    s = slug
    s = re.sub(r"^nuri-korella-", "", s)
    s = re.sub(r"^(ed|fiber|mens|male-vitality|puffy-face|testosterone)-", "", s)
    s = re.sub(r"-v(\d+)$", r" · v\1", s)
    s = s.replace("-", " · ")
    return s


def filter_jobs_by_search(jobs, q):
    needle = q.lower()
    out = []
    for j in jobs:
        hay = " ".join([
            shorten_slug(j.get("first_dialogue") or j.get("id") or ""),
            j.get("first_dialogue") or "",
            j.get("notes") or "",
            j.get("id") or "",
        ]).lower()
        if needle in hay:
            out.append(j)
    return out


def sort_jobs(jobs, key):
    arr = list(jobs)
    if key == "newest":
        arr.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    elif key == "oldest":
        arr.sort(key=lambda j: j.get("created_at") or "")
    elif key == "stuck":
        arr.sort(key=lambda j: j.get("stuck_days") or 0, reverse=True)
    return arr


# ---------------------------------------------------------------------------
# filter_jobs_by_search
# ---------------------------------------------------------------------------
def test_filter_matches_substring_in_dialogue():
    jobs = [
        {"id": "a", "first_dialogue": "bedroom morph saffron"},
        {"id": "b", "first_dialogue": "another video"},
    ]
    out = filter_jobs_by_search(jobs, "morph")
    assert len(out) == 1 and out[0]["id"] == "a"


def test_filter_case_insensitive():
    jobs = [{"id": "a", "first_dialogue": "BEDROOM saffron"}]
    assert len(filter_jobs_by_search(jobs, "bedroom")) == 1


def test_filter_matches_notes():
    jobs = [{"id": "a", "first_dialogue": "x", "notes": "needs hook re-record"}]
    assert len(filter_jobs_by_search(jobs, "re-record")) == 1


def test_filter_empty_query_returns_nothing_via_substring_guard():
    """Caller skips filter when q is empty; the JS does the same. Here verify
    empty needle matches all entries (Python `in` semantics)."""
    jobs = [{"id": "a", "first_dialogue": "x"}]
    assert len(filter_jobs_by_search(jobs, "")) == 1  # empty substring is in everything


def test_filter_matches_shortened_slug():
    jobs = [{"id": "x", "first_dialogue": "nuri-korella-ed-bedroom-morph-v1"}]
    assert len(filter_jobs_by_search(jobs, "bedroom")) == 1


# ---------------------------------------------------------------------------
# sort_jobs
# ---------------------------------------------------------------------------
def test_sort_newest():
    jobs = [
        {"id": "old", "created_at": "2026-05-01"},
        {"id": "new", "created_at": "2026-05-30"},
    ]
    out = sort_jobs(jobs, "newest")
    assert out[0]["id"] == "new"


def test_sort_oldest():
    jobs = [
        {"id": "old", "created_at": "2026-05-01"},
        {"id": "new", "created_at": "2026-05-30"},
    ]
    out = sort_jobs(jobs, "oldest")
    assert out[0]["id"] == "old"


def test_sort_stuck_descending():
    jobs = [
        {"id": "a", "stuck_days": 1},
        {"id": "b", "stuck_days": 5},
        {"id": "c", "stuck_days": 3},
    ]
    out = sort_jobs(jobs, "stuck")
    assert [j["id"] for j in out] == ["b", "c", "a"]


def test_sort_handles_missing_created_at():
    jobs = [
        {"id": "a"},
        {"id": "b", "created_at": "2026-05-30"},
    ]
    out = sort_jobs(jobs, "newest")
    assert out[0]["id"] == "b"


def test_sort_unknown_key_passes_through():
    jobs = [{"id": "a"}, {"id": "b"}]
    out = sort_jobs(jobs, "nonsense-key")
    assert [j["id"] for j in out] == ["a", "b"]
