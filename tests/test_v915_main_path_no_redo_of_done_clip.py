# v915 — the MAIN-path hard-failure requeue must not redo a clip the platform
# already has a render for.
#
# Measured 2026-08-07, job 4f8687cf:
#   Clip 13972 status -> completed          x2
#   Clip 13972 status -> flow_redo_queued   x5
#   Clip 13972 status -> generating         x6
#
# A finished clip was regenerated over and over, burning a golden-restore cycle
# each time while other clips waited behind it. That is the operator's original
# complaint ("these videos are done, why are they being requeued? and why not
# other clips are being generated?") reappearing through a second door.
#
# v913 fixed clip_done_in_platform() itself, and the HTTP-DL give-up path calls
# it - but this MAIN-path delayed-hard-failure requeue never did. The DOM
# re-verify can read the wrong tile after a golden restore, so "hard failure"
# here is not evidence the render is missing; the platform is the authority.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _branch(src):
    i = src.index("[v915] clip")
    return src[i - 1200: i + 1200]


class TestV915MainPathNoRedoOfDoneClip(unittest.TestCase):
    def test_platform_check_runs_before_the_requeue(self):
        src = _source()
        i_guard = src.index("if clip_done_in_platform(_df_clip['id']):")
        i_requeue = src.index("_df_cycles = register_auto_redo_cycle(_df_clip['id'])")
        self.assertLess(i_guard, i_requeue,
                        "the platform check must run BEFORE the clip is re-queued")

    def test_done_clip_skips_without_consuming_a_retry(self):
        b = _branch(_source())
        self.assertIn("continue", b, "a done clip must skip the requeue entirely")
        self.assertIn("clear_auto_redo_cycle", b,
                      "a false hard-failure must not leave retry budget consumed")

    def test_real_failure_still_requeues(self):
        """A clip with no render in the platform must still redo, or genuine
        failures would never recover."""
        src = _source()
        self.assertIn("update_clip_status(_df_clip['id'], 'flow_redo_queued'", src)
        self.assertIn("register_auto_redo_cycle(_df_clip['id'])", src)

    def test_guard_uses_the_v913_corrected_helper(self):
        """clip_done_in_platform now trusts has_video (v913); this path must use
        that same helper rather than re-deriving 'done' from clip.status."""
        src = _source()
        self.assertIn("def clip_done_in_platform(", src)
        self.assertIn("if _f.get('has_video'):", src)


if __name__ == "__main__":
    unittest.main()
