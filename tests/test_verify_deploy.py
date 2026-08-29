import contextlib
import io
import unittest
from unittest import mock

import verify_deploy


class VerifyDeployTests(unittest.TestCase):
    def test_matching_commit_must_also_be_healthy(self):
        replies = iter(
            [
                {"render_commit": "abcdef0123456789", "status": "degraded"},
                {"render_commit": "abcdef0123456789", "status": "healthy"},
            ]
        )
        ticks = iter([0, 0, 1, 1, 2])
        output = io.StringIO()
        with (
            mock.patch.object(verify_deploy, "probe", side_effect=lambda _url: next(replies)),
            mock.patch.object(verify_deploy.time, "time", side_effect=lambda: next(ticks)),
            mock.patch.object(verify_deploy.time, "sleep"),
            contextlib.redirect_stdout(output),
        ):
            result = verify_deploy.main(
                ["verify_deploy.py", "abcdef0", "--timeout", "2", "--interval", "1"]
            )

        self.assertEqual(result, 0)
        self.assertIn("not healthy yet", output.getvalue())
        self.assertIn("live and healthy", output.getvalue())

    def test_invalid_sha_is_rejected(self):
        self.assertEqual(verify_deploy.main(["verify_deploy.py", "not-a-sha"]), 2)


if __name__ == "__main__":
    unittest.main()


# --- v953: ancestry, not equality ------------------------------------------
# The watcher asked "does the live sha EQUAL mine?". On a repo several sessions
# push to, the live TIP is routinely a DESCENDANT of what you pushed. Measured
# 2026-08-29: four commits were live and serving while this said NOT CONFIRMED,
# and deploy.ps1 turned that into "the live deploy was not confirmed healthy".

def _resolve(rev):
    import subprocess, os
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    r = subprocess.run(["git", "-C", repo, "rev-parse", rev],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def test_a_descendant_live_sha_confirms():
    import pytest
    import verify_deploy
    mine, live = _resolve("f9d7c9d"), _resolve("f798f948")
    if not mine or not live:
        pytest.skip("this checkout does not have the 2026-08-29 commits")
    assert verify_deploy.live_contains(mine, live) is True


def test_undecidable_never_confirms():
    """'Cannot tell' must stay unconfirmed, so the timeout still returns 1.
    Collapsing None into True is how a fail-closed gate stops failing."""
    import verify_deploy
    assert verify_deploy.live_contains("24cd1f0", "") is None
    assert verify_deploy.live_contains("", "24cd1f0") is None
    assert verify_deploy.live_contains("24cd1f0", "deadbeef" * 5) is None


def test_the_falsifier_a_later_commit_is_not_inside_an_earlier_one():
    import pytest
    import verify_deploy
    mine, live = _resolve("f9d7c9d"), _resolve("f798f948")
    if not mine or not live:
        pytest.skip("this checkout does not have the 2026-08-29 commits")
    assert verify_deploy.live_contains(live, mine) is False
