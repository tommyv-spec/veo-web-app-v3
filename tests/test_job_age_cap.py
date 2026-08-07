"""Worker claim age cap — jobs older than the window are never claimed."""
import os, sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from job_age import max_job_age_days, job_age_cutoff


def test_default_window_is_7_days():
    assert max_job_age_days({}) == 7


def test_env_override():
    assert max_job_age_days({"WORKER_MAX_JOB_AGE_DAYS": "30"}) == 30


def test_zero_disables_the_cap():
    assert max_job_age_days({"WORKER_MAX_JOB_AGE_DAYS": "0"}) == 0
    assert job_age_cutoff({"WORKER_MAX_JOB_AGE_DAYS": "0"}) is None


def test_garbage_env_falls_back_to_default():
    assert max_job_age_days({"WORKER_MAX_JOB_AGE_DAYS": "abc"}) == 7
    assert max_job_age_days({"WORKER_MAX_JOB_AGE_DAYS": ""}) == 7


def test_negative_env_falls_back_to_default():
    assert max_job_age_days({"WORKER_MAX_JOB_AGE_DAYS": "-3"}) == 7


def test_cutoff_is_now_minus_window():
    now = datetime(2026, 8, 7, 12, 0, 0)
    cutoff = job_age_cutoff({}, now=now)
    assert cutoff == now - timedelta(days=7)
