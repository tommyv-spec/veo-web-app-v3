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
