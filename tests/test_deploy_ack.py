# tests/test_deploy_ack.py
#
# The deploy gate after the v898 split (2026-08-03, operator-approved).
#
# History: the original line-loss check did two jobs with one blunt test —
# catching stale-tree rewinds AND forcing review of deliberate removals. Every
# FAIL after install was the second job firing on reviewed, tested replacements
# (rev-238 import contortion, rev-240 "additive-only" rework, one reworded
# comment line, three composer-click lines), while the first job had a cheap
# exact test the gate never used: ancestry.
#
# The contract under test now:
#   * ANCESTRY — a candidate that does not descend from protected main fails
#     HARD and is never acknowledgeable (the 2026-07-30 rewind incident class)
#   * line changes on a DESCENDANT tree pass, printed as a REPLACEMENT
#     ACCOUNTING (lost line next to its closest added line; VANISHED when
#     nothing similar was added) for the deploy log
#   * DELETED FILES keep the --ack ceremony: scoped to the exact tree + exact
#     loss set, staled by any new commit or any change to the set
#   * syntax errors are never acknowledgeable, before or after an ack
#   * an ack answers deletions only — it never suppresses other checks

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


class DeployGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        self.base = make_repo(self.repo)
        # the refactor: reword one line — the classic pre-v898 false positive
        (self.repo / "notes.md").write_text(
            "reworded note line\nsecond note line\n", encoding="utf-8")
        self.candidate = commit(self.repo, "reword one line")

    def tearDown(self):
        self.temp.cleanup()

    def ack_file(self):
        return self.repo / ".deploy_ack.json"

    def delete_notes(self):
        run(self.repo, "rm", "-q", "notes.md")
        return commit(self.repo, "delete notes.md")

    # ---- the v898 behaviour change: edits pass, with accounting ----------

    def test_replaced_line_passes_with_accounting(self):
        result = check(self.repo, self.candidate, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("REPLACEMENT ACCOUNTING", result.stdout)
        self.assertIn("original note line", result.stdout)
        self.assertIn("-> reworded note line", result.stdout)

    def test_vanished_line_passes_but_is_flagged(self):
        (self.repo / "notes.md").write_text(
            "reworded note line\n", encoding="utf-8")  # second line gone, nothing added
        gone_line = commit(self.repo, "drop second line")
        result = check(self.repo, gone_line, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("VANISHED", result.stdout)
        self.assertIn("second note line", result.stdout)

    # ---- ancestry: the stale-tree tripwire -------------------------------

    def test_stale_tree_fails_hard(self):
        # pushing base while "main" is already at candidate = a rewind
        result = check(self.repo, self.base, self.candidate)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STALE TREE", result.stdout)
        self.assertIn("Rebase", result.stdout)

    def test_stale_tree_is_never_acknowledgeable(self):
        result = check(self.repo, self.base, self.candidate, "--ack")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("never acknowledgeable", result.stdout)
        self.assertFalse(self.ack_file().exists(),
                         "an ack file must not be written for a stale tree")

    # ---- deletions keep the scoped-ack ceremony --------------------------

    def test_deletion_fails_without_ack(self):
        deleted = self.delete_notes()
        result = check(self.repo, deleted, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("DELETED FILES", result.stdout)
        self.assertIn("--ack", result.stdout)  # the fix is advertised at the failure

    def test_deletion_ack_writes_scoped_file_and_passes(self):
        deleted = self.delete_notes()
        result = check(self.repo, deleted, self.base, "--ack")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ACK WRITTEN", result.stdout)
        data = json.loads(self.ack_file().read_text(encoding="utf-8"))
        tree = run(self.repo, "rev-parse", "%s^{tree}" % deleted)
        self.assertEqual(data["tree"], tree)
        self.assertEqual(data["deleted_files"], 1)
        # the exact call shape the pre-push hook and deploy.ps1 make: no flags
        rerun = check(self.repo, deleted, self.base)
        self.assertEqual(rerun.returncode, 0, rerun.stdout + rerun.stderr)
        self.assertIn("scoped to this exact tree", rerun.stdout)

    def test_new_commit_stales_the_deletion_ack(self):
        deleted = self.delete_notes()
        check(self.repo, deleted, self.base, "--ack")
        (self.repo / "extra.md").write_text("new file\n", encoding="utf-8")
        newer = commit(self.repo, "another change")
        result = check(self.repo, newer, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("STALE ACK", result.stdout)

    def test_mixed_deletion_and_edit_ack_covers_both(self):
        # the ack fingerprints line losses too, so an ack written by this
        # version still satisfies the pre-v898 checker copy the pre-push hook
        # runs until this version lands on main
        deleted = self.delete_notes()
        result = check(self.repo, deleted, self.base, "--ack")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        data = json.loads(self.ack_file().read_text(encoding="utf-8"))
        self.assertIn("fingerprint", data)

    # ---- never acknowledgeable / edge cases ------------------------------

    def test_syntax_error_is_never_acknowledgeable(self):
        (self.repo / "app.py").write_text("def broken(:\n", encoding="utf-8")
        broken = commit(self.repo, "break python")
        result = check(self.repo, broken, self.base, "--ack")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("never acknowledgeable", result.stdout)
        self.assertFalse(self.ack_file().exists(),
                         "an ack file must not be written past a syntax failure")

    def test_ack_with_no_change_is_a_noop(self):
        result = check(self.repo, self.base, self.base, "--ack")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("nothing to acknowledge", result.stdout)
        self.assertFalse(self.ack_file().exists())

    def test_corrupt_ack_file_is_ignored_not_fatal(self):
        deleted = self.delete_notes()
        self.ack_file().write_text("{not json", encoding="utf-8")
        result = check(self.repo, deleted, self.base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("DELETED FILES", result.stdout)

    def test_allow_loss_still_allows_deletion(self):
        deleted = self.delete_notes()
        result = check(self.repo, deleted, self.base, "--allow-loss")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("--allow-loss", result.stdout)


if __name__ == "__main__":
    unittest.main()
