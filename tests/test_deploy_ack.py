# tests/test_deploy_ack.py
#
# The scoped deploy acknowledgment (`check_deploy_safety.py --ack`).
#
# Why it exists: --allow-loss was a blanket flag that neither the pre-push hook
# nor deploy.ps1 passed through, so a deliberate refactor (any moved/reworded
# line reads as "loss") forced either `git push --no-verify` — skipping EVERY
# check — or contorting code to fake zero loss. Both happened in practice
# (2026-08-03, and the documented restore-the-line workaround before that).
#
# The contract under test:
#   * --ack writes .deploy_ack.json bound to the exact TREE + exact loss set
#   * a plain re-run (what the hook and deploy.ps1 execute) then PASSES
#   * any new commit stales it; any change to the loss set stales it
#   * syntax errors are never acknowledgeable, before or after an ack
#   * an ack is only ever an answer to loss — it never suppresses other checks

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "check_deploy_safety.py"


def run(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=True,
    ).stdout.strip()


def commit(repo, message):
    run(repo, "add", "-A")
    run(repo, "commit", "-m", message)
    return run(repo, "rev-parse", "HEAD")


def make_repo(repo):
    run(repo, "init")
    run(repo, "config", "user.email", "deploy-test@example.com")
    run(repo, "config", "user.name", "Deploy Test")
    (repo / "app.py").write_text("print('keep')\n", encoding="utf-8")
    (repo / "notes.md").write_text("original note line\nsecond note line\n", encoding="utf-8")
    return commit(repo, "base")


def check(repo, candidate, base, *extra):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--ref", candidate, "--main", base, *extra],
        cwd=repo, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )


class DeployAckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        self.base = make_repo(self.repo)
        # the refactor: reword one line — the classic false-positive "loss"
        (self.repo / "notes.md").write_text(
            "reworded note line\nsecond note line\n", encoding="utf-8")
        self.candidate = commit(self.repo, "reword one line")

    def tearDown(self):
        self.temp.cleanup()

    def ack_file(self):
        return self.repo / ".deploy_ack.json"

    def test_loss_fails_without_ack(self):
        result = check(self.repo, self.candidate, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("CONTENT LOSS", result.stdout)
        self.assertIn("--ack", result.stdout)  # the fix is advertised at the point of failure

    def test_ack_writes_scoped_file_and_passes(self):
        result = check(self.repo, self.candidate, self.base, "--ack")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ACK WRITTEN", result.stdout)
        data = json.loads(self.ack_file().read_text(encoding="utf-8"))
        tree = run(self.repo, "rev-parse", "%s^{tree}" % self.candidate)
        self.assertEqual(data["tree"], tree)
        self.assertEqual(data["lost_lines"], 1)

    def test_plain_rerun_honors_ack(self):
        # This is the exact call shape the pre-push hook and deploy.ps1 make:
        # no flags. It must pass once the ack exists.
        check(self.repo, self.candidate, self.base, "--ack")
        result = check(self.repo, self.candidate, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("scoped to this exact tree", result.stdout)

    def test_new_commit_stales_the_ack(self):
        check(self.repo, self.candidate, self.base, "--ack")
        (self.repo / "extra.md").write_text("new file\n", encoding="utf-8")
        newer = commit(self.repo, "another change")
        result = check(self.repo, newer, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STALE ACK", result.stdout)

    def test_changed_loss_set_stales_the_ack(self):
        check(self.repo, self.candidate, self.base, "--ack")
        # widen the loss: now the second line is reworded too
        (self.repo / "notes.md").write_text(
            "reworded note line\nreworded second line\n", encoding="utf-8")
        wider = commit(self.repo, "reword second line too")
        result = check(self.repo, wider, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STALE ACK", result.stdout)

    def test_syntax_error_is_never_acknowledgeable(self):
        (self.repo / "app.py").write_text("def broken(:\n", encoding="utf-8")
        broken = commit(self.repo, "break python")
        result = check(self.repo, broken, self.base, "--ack")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("never acknowledgeable", result.stdout)
        self.assertFalse(self.ack_file().exists(),
                         "an ack file must not be written past a syntax failure")

    def test_ack_with_no_loss_is_a_noop(self):
        result = check(self.repo, self.base, self.base, "--ack")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("nothing to acknowledge", result.stdout)
        self.assertFalse(self.ack_file().exists())

    def test_corrupt_ack_file_is_ignored_not_fatal(self):
        self.ack_file().write_text("{not json", encoding="utf-8")
        result = check(self.repo, self.candidate, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("CONTENT LOSS", result.stdout)

    def test_allow_loss_still_works_unchanged(self):
        result = check(self.repo, self.candidate, self.base, "--allow-loss")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("loss acknowledged via --allow-loss", result.stdout)


if __name__ == "__main__":
    unittest.main()
