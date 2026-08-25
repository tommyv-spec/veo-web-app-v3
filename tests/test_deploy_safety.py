import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import check_deploy_safety


SCRIPT = Path(__file__).resolve().parents[1] / "check_deploy_safety.py"


def run(repo, *args):
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    ).stdout.strip()


def commit(repo, message):
    run(repo, "add", "-A")
    run(repo, "commit", "-m", message)
    return run(repo, "rev-parse", "HEAD")


def make_repo(repo):
    run(repo, "init")
    run(repo, "config", "user.email", "deploy-test@example.com")
    run(repo, "config", "user.name", "Deploy Test")
    (repo / "app.py").write_text(
        "print('keep')\nprint('duplicate')\nprint('duplicate')\n", encoding="utf-8"
    )
    (repo / "notes.md").write_text("keep this note\n", encoding="utf-8")
    return commit(repo, "base")


def check(repo, candidate, base, *extra):
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--ref", candidate, "--main", base, *extra],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


class DeploySafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp.name)
        self.base = make_repo(self.repo)

    def tearDown(self):
        self.temp.cleanup()

    def test_same_tree_passes(self):
        result = check(self.repo, self.base, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("RESULT: PASS", result.stdout)

    def test_all_deploy_mechanism_files_are_treated_as_text(self):
        for path in (
            "deploy.ps1",
            "Dockerfile",
            ".gitignore",
            "static/site.webmanifest",
            "git-hooks/pre-push-code",
        ):
            self.assertTrue(check_deploy_safety.is_text_path(path), path)

    def test_deliberate_line_replacement_is_reported_but_passes(self):
        (self.repo / "notes.md").write_text("replacement note\n", encoding="utf-8")
        candidate = commit(self.repo, "remove line")
        result = check(self.repo, candidate, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("REPLACEMENT ACCOUNTING", result.stdout)

    def test_removed_duplicate_is_reported_but_passes(self):
        (self.repo / "app.py").write_text("print('keep')\nprint('duplicate')\n", encoding="utf-8")
        candidate = commit(self.repo, "remove one duplicate")
        result = check(self.repo, candidate, self.base)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("app.py (1)", result.stdout)

    def test_broken_changed_python_blocks_even_when_loss_is_allowed(self):
        (self.repo / "app.py").write_text("def broken(:\n", encoding="utf-8")
        candidate = commit(self.repo, "break python")
        result = check(self.repo, candidate, self.base, "--allow-loss")
        self.assertEqual(result.returncode, 1)
        self.assertIn("SYNTAX ERRORS", result.stdout)

    def test_unreadable_base_blocks(self):
        result = check(self.repo, self.base, "missing-main-ref")
        self.assertEqual(result.returncode, 2)
        self.assertIn("deploy blocked", result.stdout)

    def test_deleted_text_file_blocks(self):
        (self.repo / "notes.md").unlink()
        candidate = commit(self.repo, "delete notes")
        result = check(self.repo, candidate, self.base)
        self.assertEqual(result.returncode, 1)
        self.assertIn("DELETED FILES", result.stdout)

    def test_deliberate_loss_can_be_acknowledged_but_is_still_reported(self):
        (self.repo / "notes.md").unlink()
        candidate = commit(self.repo, "delete notes")
        result = check(self.repo, candidate, self.base, "--allow-loss")
        self.assertEqual(result.returncode, 0)
        self.assertIn("deletion allowed via --allow-loss", result.stdout)

    def test_malformed_script_markup_blocks_html_deploy(self):
        static = self.repo / "static"
        static.mkdir()
        page = static / "index.html"
        page.write_text(
            "<html><script>const ok = true;</script></html>\n", encoding="utf-8"
        )
        base = commit(self.repo, "add valid page")
        page.write_text("<html><script const broken = ;</html>\n", encoding="utf-8")
        candidate = commit(self.repo, "break script markup")

        result = check(self.repo, candidate, base)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("no complete <script>...</script> block", result.stdout)
        self.assertIn("RESULT: FAIL", result.stdout)

    def test_removing_all_script_markup_from_scripted_page_blocks(self):
        static = self.repo / "static"
        static.mkdir()
        page = static / "index.html"
        page.write_text(
            "<html><script>const ok = true;</script></html>\n", encoding="utf-8"
        )
        base = commit(self.repo, "add scripted page")
        page.write_text("<html>const broken = ;</html>\n", encoding="utf-8")
        candidate = commit(self.repo, "remove script markup")

        result = check(self.repo, candidate, base)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("no complete <script>...</script> block", result.stdout)


if __name__ == "__main__":
    unittest.main()
