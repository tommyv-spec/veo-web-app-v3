"""Tests for the burned-caption brand speller.

WHY THIS FILE EXISTS. `docs/protected-words.json` records 2026-09-04: "a finished,
gate-passing selling video shipped with the brand spelled 'Corella' in three burned
captions". A delivery gate was built for it and it works -- it blocked nuri
1984be6c on 2026-09-17 for the same word. But a gate only stops a bad delivery;
something still has to SPELL IT RIGHT, and nothing did, so every video with the
brand spoken aloud needed a human to catch it. These tests pin the speller.

The rule being pinned is narrow on purpose: ONLY the exact wrong spellings already
recorded in the vocabulary are replaced. A transcriber that writes a genuinely
different word must stay visible rather than be silently rewritten into the brand.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import autoedit_captions as ac  # noqa: E402


class BrandSpeller(unittest.TestCase):
    def test_the_vocabulary_actually_loads(self):
        """A silent empty map would make every other test here pass for nothing."""
        fixes = ac._brand_fixes()
        self.assertIn("corella", fixes)
        self.assertEqual(fixes["corella"], "Korella")

    def test_the_spelling_that_shipped_twice_is_corrected(self):
        out = ac.fix_brand_words([(0.0, 0.5, "corella")])
        self.assertEqual(out[0][2], "korella")

    def test_case_style_and_timings_survive(self):
        out = ac.fix_brand_words([(1.0, 2.0, "CORELLA"), (2.0, 3.0, "Corella")])
        self.assertEqual(out[0][2], "KORELLA")
        self.assertEqual(out[1][2], "Korella")
        self.assertEqual((out[0][0], out[0][1]), (1.0, 2.0))

    def test_punctuation_is_kept(self):
        out = ac.fix_brand_words([(0.0, 0.5, "CORELLA,")])
        self.assertEqual(out[0][2], "KORELLA,")

    def test_every_recorded_misspelling_is_covered(self):
        wrongs = ["corella", "xorella", "korela", "kore11a", "xorclla"]
        out = ac.fix_brand_words([(0.0, 0.1, w) for w in wrongs])
        self.assertTrue(all(w[2].lower() == "korella" for w in out), out)

    def test_an_unrelated_word_is_left_alone(self):
        """The narrowness IS the safety property: no fuzzy matching."""
        for word in ("saffron", "cortisol", "umbrella", "cinderella"):
            out = ac.fix_brand_words([(0.0, 0.1, word)])
            self.assertEqual(out[0][2], word)

    def test_a_missing_vocabulary_never_breaks_a_render(self):
        real = ac._VOCAB
        try:
            ac._VOCAB = Path("no-such-file-anywhere.json")
            words = [(0.0, 0.1, "corella")]
            self.assertEqual(ac.fix_brand_words(words), words)
        finally:
            ac._VOCAB = real


if __name__ == "__main__":
    unittest.main()
