# v903 — the lean golden must ship a real Chrome variations seed.
#
# flow_worker's chrome_warmup states the rule: "reCAPTCHA Enterprise checks
# x-client-data - a short header = low trust score = 403". Chrome only sends
# x-client-data when it has an applied variations seed, stored in
# Local State.variations_compressed_seed.
#
# Measured 2026-08-06 on this machine:
#   real Chrome stable : variations_compressed_seed = 61292 chars
#   real Chrome Beta   : MISSING          <- the channel the profile is pulled from
#   worker golden      : MISSING, but variations_seed_signature/_date PRESENT
#
# The copy faithfully preserved Beta's absence, so worker Chrome sent NO
# x-client-data ("[Warmup] no x-client-data header captured") and EVERY generate
# returned 403 "reCAPTCHA evaluation failed" / PUBLIC_ERROR_UNUSUAL_ACTIVITY -
# which the worker misread as an account block and golden-restored forever.

import json
import os
import tempfile
import unittest

PULL = os.path.join(os.path.dirname(__file__), "..", "static", "worker_profile_pull.py")


def _source():
    with open(PULL, encoding="utf-8") as f:
        return f.read()


class TestV903VariationsSeed(unittest.TestCase):
    def test_builder_borrows_a_seed_when_the_source_has_none(self):
        src = _source()
        self.assertIn("variations_compressed_seed", src,
                      "the lean-golden builder no longer handles the variations seed")
        self.assertIn("resolve_laptop_user_data_dirs()", src,
                      "the donor search must scan the other Chrome installs")

    def test_whole_variations_set_is_copied_together(self):
        """The signature signs the seed - a mismatched pair is worse than none,
        so every variations_* key must move as one unit."""
        src = _source()
        i = src.index("_VAR_KEYS = (")
        block = src[i:i + 400]
        for key in ("variations_compressed_seed", "variations_seed_signature",
                    "variations_seed_date", "variations_country"):
            self.assertIn(key, block, f"{key} missing from the copied set")
        # stale keys from the seedless source must be dropped before copying
        self.assertIn("ls.pop(_k, None)", src,
                      "stale variations keys must be cleared so no mismatched pair survives")

    def test_only_fills_in_when_source_seed_is_absent(self):
        """A source that already has a good seed must be left alone."""
        src = _source()
        i = src.index('_seed = (ls.get("variations_compressed_seed") or "")')
        self.assertIn("if not _seed:", src[i:i + 200],
                      "the borrow must be conditional on the source lacking a seed")

    def test_donor_selection_logic(self):
        """Exercise the selection rule itself: pick the first install that
        actually has a non-empty seed, skipping the source dir."""
        with tempfile.TemporaryDirectory() as td:
            src_dir = os.path.join(td, "beta"); os.makedirs(src_dir)
            good = os.path.join(td, "stable"); os.makedirs(good)
            empty = os.path.join(td, "canary"); os.makedirs(empty)
            with open(os.path.join(src_dir, "Local State"), "w") as f:
                json.dump({"variations_seed_signature": "sig-no-seed"}, f)
            with open(os.path.join(empty, "Local State"), "w") as f:
                json.dump({"variations_compressed_seed": ""}, f)
            with open(os.path.join(good, "Local State"), "w") as f:
                json.dump({"variations_compressed_seed": "REAL", "variations_seed_signature": "s"}, f)

            donor = None
            for cand in (src_dir, empty, good):
                if cand == src_dir:
                    continue
                p = os.path.join(cand, "Local State")
                if not os.path.isfile(p):
                    continue
                with open(p) as f:
                    d = json.load(f)
                if d.get("variations_compressed_seed"):
                    donor = (cand, d)
                    break
            self.assertIsNotNone(donor, "a seeded install must be selected")
            self.assertEqual(good, donor[0], "the empty-seed install must be skipped")


if __name__ == "__main__":
    unittest.main()
