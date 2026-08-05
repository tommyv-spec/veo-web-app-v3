"""v891d — worker-side wedge guard (_mark_loop_alive / _loop_wedged_secs).

The heartbeat threads consult _loop_wedged_secs() before beating; a fresh
stamp must read 0 and a stale one must read its true age so the beat gets
suppressed and the platform light goes red instead of lying.
"""
import time

import image_worker as iw


def test_fresh_stamp_reads_zero():
    iw._mark_loop_alive()
    assert iw._loop_wedged_secs() == 0


def test_mark_loop_alive_updates_stamp():
    iw._LOOP_ALIVE["t"] = 12345.0
    iw._mark_loop_alive()
    assert iw._LOOP_ALIVE["t"] > 12345.0
    assert abs(iw._LOOP_ALIVE["t"] - time.time()) < 5


def test_stale_stamp_reads_its_age():
    iw._LOOP_ALIVE["t"] = time.time() - (iw.WEDGE_AFTER_S + 120)
    age = iw._loop_wedged_secs()
    assert age > iw.WEDGE_AFTER_S
    assert abs(age - (iw.WEDGE_AFTER_S + 120)) < 5
    iw._mark_loop_alive()  # restore for other tests


def test_just_under_threshold_reads_zero():
    iw._LOOP_ALIVE["t"] = time.time() - (iw.WEDGE_AFTER_S - 60)
    assert iw._loop_wedged_secs() == 0
    iw._mark_loop_alive()
