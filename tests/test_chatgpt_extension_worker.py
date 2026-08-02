import io
import json
import unittest
import zipfile
from pathlib import Path

from chatgpt_extension_bundle import build_extension_zip
from chatgpt_extension_pairing import (
    InvalidPairingTicket,
    load_ticket,
    make_ticket,
    normalize_email,
)


EXTENSION_DIR = Path(__file__).parents[1] / "static" / "chatgpt_extension"


class ChatgptExtensionWorkerTests(unittest.TestCase):
    def test_manifest_uses_only_small_required_permission_set(self):
        manifest = json.loads((EXTENSION_DIR / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["manifest_version"], 3)
        self.assertEqual(set(manifest["permissions"]), {"storage", "tabs"})
        self.assertNotIn("cookies", manifest["permissions"])
        self.assertEqual(
            set(manifest["host_permissions"]),
            {"https://kavenobuilder.com/*", "https://chatgpt.com/*"},
        )

    def test_extension_has_no_browser_close_or_profile_copy_path(self):
        source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in EXTENSION_DIR.iterdir()
            if path.suffix == ".js"
        ).lower()

        for forbidden in (
            "chrome.windows.remove",
            "chrome.tabs.remove",
            "user-data-dir",
            "profile-directory",
            "cookie.getall",
        ):
            self.assertNotIn(forbidden, source)

    def test_extension_bundle_contains_every_declared_file(self):
        manifest = json.loads((EXTENSION_DIR / "manifest.json").read_text(encoding="utf-8"))
        expected = {
            "manifest.json",
            manifest["background"]["service_worker"],
            "worker.html",
            "worker.css",
            "worker.js",
        }
        for content_script in manifest["content_scripts"]:
            expected.update(content_script["js"])

        actual = {path.name for path in EXTENSION_DIR.iterdir() if path.is_file()}
        self.assertTrue(expected.issubset(actual), expected - actual)

    def test_download_bundle_is_a_root_level_extension_zip(self):
        archive = zipfile.ZipFile(io.BytesIO(build_extension_zip(EXTENSION_DIR)))
        names = set(archive.namelist())

        self.assertIn("manifest.json", names)
        self.assertIn("worker.js", names)
        self.assertFalse(any(name.startswith("chatgpt_extension/") for name in names))

    def test_pair_ticket_round_trip_normalizes_email(self):
        ticket = make_ticket(
            "test-secret", "user-123", " Person+ChatGPT@Example.COM ", "https://kavenobuilder.com/"
        )

        payload = load_ticket("test-secret", ticket, max_age_s=600)

        self.assertEqual(payload["user_id"], "user-123")
        self.assertEqual(payload["email"], "person+chatgpt@example.com")
        self.assertEqual(payload["api_url"], "https://kavenobuilder.com")

    def test_pair_ticket_rejects_tampering(self):
        ticket = make_ticket(
            "test-secret", "user-123", "person@example.com", "https://kavenobuilder.com"
        )

        with self.assertRaises(InvalidPairingTicket):
            load_ticket("test-secret", ticket + "changed", max_age_s=600)

    def test_invalid_account_email_is_rejected(self):
        for value in ("", "not-an-email", "person@localhost", "a b@example.com"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_email(value)


if __name__ == "__main__":
    unittest.main()
