import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "verify_decode_format.py"
SPEC = importlib.util.spec_from_file_location("verify_decode_format", MODULE_PATH)
verify_decode_format = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_decode_format)


def decode_with_ledger(rows):
    return f"""## Pre-Flight Decode Checklist

## Ingredients

## Images
### Image 1

## Storyboard
### Scene 1

## Comprehension

## Adaptation-extraction
register proxy chain angle

### Hero-symptom intensity ledger

| Hero symptom / carrier | Literal observed scale + comparison anchor | Intensity | Exaggeration headroom |
|---|---|---|---|
{rows}

## Veo 3.1 Final Prompts
### Clip 1.1
**Text prompt:**
"""


class VerifyDecodeIntensityLedgerTests(unittest.TestCase):
    def lint_text(self, text):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "decoded_fixture.md"
            path.write_text(text, encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = verify_decode_format.lint(path)
            return result, output.getvalue()

    def test_valid_viral_max_with_no_headroom_passes(self):
        row = "| enormous belly / adult man | fills the lower two-thirds of the frame and dwarfs his chest | 5/5 viral-max | NO |"
        result, output = self.lint_text(decode_with_ledger(row))
        self.assertEqual(0, result, output)

    def test_valid_mid_intensity_with_headroom_passes(self):
        row = "| swollen ankle / adult woman | swelling nearly erases the ankle bone | 3/5 heavy | YES |"
        result, output = self.lint_text(decode_with_ledger(row))
        self.assertEqual(0, result, output)

    def test_explicit_no_symptom_row_passes(self):
        row = "| none observed | n/a | n/a | n/a |"
        result, output = self.lint_text(decode_with_ledger(row))
        self.assertEqual(0, result, output)

    def test_missing_ledger_fails(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace("### Hero-symptom intensity ledger", "### Other notes")
        result, output = self.lint_text(text)
        self.assertEqual(1, result)
        self.assertIn("missing `### Hero-symptom intensity ledger`", output)

    def test_vague_scale_without_comparison_fails(self):
        row = "| belly / adult man | a big bloated belly | 4/5 extreme | YES |"
        result, output = self.lint_text(decode_with_ledger(row))
        self.assertEqual(1, result)
        self.assertIn("lacks a literal comparison anchor", output)

    def test_five_of_five_with_yes_headroom_fails(self):
        row = "| belly / adult man | fills the lower two-thirds of the frame | 5/5 viral-max | YES |"
        result, output = self.lint_text(decode_with_ledger(row))
        self.assertEqual(1, result)
        self.assertIn("5/5 viral-max but says headroom YES", output)


if __name__ == "__main__":
    unittest.main()
