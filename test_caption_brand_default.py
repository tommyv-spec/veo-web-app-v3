"""The brand spelling is a PIPELINE DEFAULT, not something a build must remember.

WHY THIS FILE EXISTS. `docs/protected-words.json` records a gate-passing selling video
shipping with the brand as "Corella" in three burned captions on 2026-09-04. On
2026-09-17 the same word blocked nuri 1984be6c, and the first fix was to add
`autoedit_caption_words` to that one build — which leaves every future build one
forgotten declaration away from the identical failure. Operator, the same day: *"all
that we discover not producing should be improved at the root cause."*

So the words the DELIVERY GATE protects are now the words the caption burner spells,
for every build, automatically. These tests pin the three properties that makes true:
it applies with no declaration, a build can still override it, and a missing data file
can never fail a render.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import autoedit_pipeline as ap  # noqa: E402


class BrandCaptionDefault(unittest.TestCase):
    def test_the_vocabulary_loads(self):
        """A silent empty map would make every other test here pass for nothing."""
        fixes = ap.brand_caption_fixes()
        self.assertIn("corella", fixes)
        self.assertEqual(fixes["corella"], "Korella")

    def test_a_build_declaring_nothing_still_spells_the_brand(self):
        """THE POINT. Before this, `None, None` returned None and no fix ran."""
        plan = ap.caption_fix_plan(None, None)
        self.assertIsNotNone(plan)
        self.assertEqual(plan["words"]["corella"], "Korella")

    def test_the_misspelling_that_shipped_is_corrected_end_to_end(self):
        plan = ap.caption_fix_plan(None, None)
        self.assertEqual(ap.fix_caption_word("CORELLA", plan), "Korella")

    def test_punctuation_survives_the_fix(self):
        plan = ap.caption_fix_plan(None, None)
        self.assertEqual(ap.fix_caption_word("corella,", plan), "Korella,")

    def test_a_build_override_beats_the_default(self):
        """A human saying 'this one is different' must outrank a default."""
        plan = ap.caption_fix_plan(None, {"corella": "Something Else"})
        self.assertEqual(plan["words"]["corella"], "Something Else")

    def test_other_declared_words_are_kept_alongside_the_defaults(self):
        plan = ap.caption_fix_plan(None, {"garnices": "Garnissa's"})
        self.assertEqual(plan["words"]["garnices"], "Garnissa's")
        self.assertEqual(plan["words"]["corella"], "Korella")

    def test_an_unrelated_word_is_untouched(self):
        """The narrowness is the safety property: exact keys, no fuzzy matching."""
        plan = ap.caption_fix_plan(None, None)
        for word in ("saffron", "cortisol", "umbrella", "cinderella"):
            self.assertEqual(ap.fix_caption_word(word, plan), word)

    def test_a_missing_vocabulary_file_never_breaks_a_render(self):
        real = ap._BRAND_VOCAB
        try:
            ap._BRAND_VOCAB = Path("no-such-file-anywhere.json")
            self.assertEqual(ap.brand_caption_fixes(), {})
            self.assertIsNone(ap.caption_fix_plan(None, None))
        finally:
            ap._BRAND_VOCAB = real

    def test_a_malformed_vocabulary_file_never_breaks_a_render(self):
        real = ap._BRAND_VOCAB
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                             encoding="utf-8") as fh:
                fh.write("{not json at all")
                bad = Path(fh.name)
            ap._BRAND_VOCAB = bad
            self.assertEqual(ap.brand_caption_fixes(), {})
            bad.unlink()
        finally:
            ap._BRAND_VOCAB = real

    def test_the_digest_changes_when_the_words_change(self):
        """A corrected transcript must not be served from a raw transcript's cache."""
        a = ap.caption_fix_digest(ap.caption_fix_plan(None, None))
        b = ap.caption_fix_digest(ap.caption_fix_plan(None, {"zzz": "Qqq"}))
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
