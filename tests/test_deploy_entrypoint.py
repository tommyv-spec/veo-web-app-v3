import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeployEntrypointTests(unittest.TestCase):
    def test_deploy_script_cannot_sweep_or_force_push(self):
        text = (ROOT / "deploy.ps1").read_text(encoding="utf-8")
        self.assertNotIn("git add", text)
        self.assertNotIn("git commit", text)
        self.assertNotIn("branch -M", text)
        self.assertNotIn("--force", text)
        self.assertIn("status --porcelain", text)
        self.assertIn("merge-base --is-ancestor origin/main HEAD", text)
        self.assertIn("check_deploy_safety.py", text)
        self.assertIn("verify_deploy.py", text)

    def test_pre_push_gate_fails_closed_and_uses_protected_checker(self):
        text = (ROOT / "git-hooks" / "pre-push-code").read_text(encoding="utf-8")
        self.assertIn('base_main="$remote_sha"', text)
        self.assertIn('git show "${base_main}:check_deploy_safety.py"', text)
        self.assertIn("remote main could not be refreshed", text)
        self.assertIn("the deploy checker did not complete", text)
        self.assertNotIn("if [ -f check_deploy_safety.py ]", text)
        # The gate must not pipe its output through text-munging tools. Match a
        # REAL pipe: a plain substring search reads the tail of `||` as `|`, so
        # `... || true` on line 127 tripped the `| tr` check and failed a push
        # that had nothing to do with `tr`. The same trap was waiting for
        # `|| grep` / `|| sed`; the lookbehind closes all three.
        for tool in ("grep", "sed", "tr"):
            self.assertIsNone(
                re.search(rf"(?<!\|)\|\s*{tool}\b", text),
                f"the pre-push gate must not pipe through {tool}")


if __name__ == "__main__":
    unittest.main()
