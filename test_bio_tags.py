"""A bio link must credit ONE profile on ONE platform, and nothing else.

Why this file exists, measured 2026-09-15/16. `BIO_TAGS` held one tag per
PERSONA, and the same `/korella/nuri` sat in both the Instagram bio and the
Facebook bio. So 284 clicks, 16 items and $550.24 landed on `kavenokorel00-20`
with no way to tell which platform earned them -- and that was 97% of the week's
money. The per-video links everyone kept looking at held almost none of it.

The fix is a bio link per profile PER PLATFORM (`/korella/nuri` for Instagram,
`/korella/nuri-fb` for Facebook), which the route already supports: it looks up
the slug and 404s anything unknown. What it could NOT do is notice two slugs
sharing a tag, which is the failure that hid this for weeks and would hide it
again the moment somebody copies a line while adding a persona.

Two things are asserted, and the second matters more than it looks:

  1. every configured slug maps to a DISTINCT tag -- a shared tag is the bug
  2. every persona that has a Facebook account is REPORTED as unsplit until its
     `-fb` slug exists, so the gap stays visible instead of passing quietly

On (2): a test that merely allowed the split would go green today, with the money
still pooled. The point of a gap test is that it keeps saying so.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Personas with a Facebook account as well as an Instagram one. Kept here rather
# than derived from BIO_TAGS, because deriving "who needs a split" from the thing
# being checked is how a missing entry proves itself correct.
PERSONAS_ON_FACEBOOK = ("nuri", "noemi", "martha")


class BioTagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import main
        cls.tags = main.BIO_TAGS

    def test_no_two_bio_links_share_a_tracking_id(self):
        """Two slugs on one tag is exactly the bug: the clicks become unsplittable."""
        seen: dict[str, str] = {}
        clashes = []
        for slug, tag in self.tags.items():
            if tag in seen:
                clashes.append(f"{seen[tag]} and {slug} both credit {tag}")
            seen[tag] = slug
        self.assertEqual([], clashes, "; ".join(clashes))

    def test_every_tag_looks_like_an_amazon_tracking_id(self):
        """An invented tag is worse than a shared one.

        `/g/<asin>/<tag>` and the bio route both echo whatever tag they are given
        -- a made-up one returns 200 and offers itself back to Amazon, where it
        credits NOBODY. Shape is the only check available offline; that the tag
        exists in the Associates account is checked by minting it there.
        """
        import re
        for slug, tag in self.tags.items():
            self.assertRegex(tag, r"^[a-z0-9][a-z0-9.-]*-\d{2}$",
                             f"{slug} -> {tag!r} is not tracking-id shaped")

    def test_the_facebook_split_is_reported_until_it_exists(self):
        """A gap test has to keep naming the gap, not go quiet when it is convenient."""
        missing = [p for p in PERSONAS_ON_FACEBOOK if f"{p}-fb" not in self.tags]
        if missing:
            self.skipTest(
                "Facebook bios still share the Instagram tag for: "
                + ", ".join(missing)
                + ". Each needs its OWN Amazon tracking id, created in the "
                  "Associates UI, then a '<persona>-fb' entry in BIO_TAGS. "
                  "Runbook: docs/bio-links-per-platform-2026-09-16.md"
            )
        # Once the entries exist, they must be real splits, not aliases.
        for persona in PERSONAS_ON_FACEBOOK:
            self.assertNotEqual(
                self.tags[persona], self.tags[f"{persona}-fb"],
                f"{persona}-fb reuses the Instagram tag, so nothing is split")

    def test_an_unknown_slug_is_refused_rather_than_guessed(self):
        """404 is the right answer: a guess would credit the wrong platform."""
        import main
        response = main.korella_bio_link("nuri-tiktok")
        self.assertEqual(404, response.status_code)


if __name__ == "__main__":
    unittest.main()
