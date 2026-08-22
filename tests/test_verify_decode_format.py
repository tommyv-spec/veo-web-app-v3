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

### Shown beats ledger

| Source beat ID | Frame / clip evidence | Shown action / process step | Meaningful objects visibly present |
|---|---|---|---|
| SB1 | clip 1, frame f_001 | holds the product beside the symptom | labeled product bottle |

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


class VerifyDecodeShownBeatsLedgerTests(unittest.TestCase):
    def lint_text(self, text):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "decoded_fixture.md"
            path.write_text(text, encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = verify_decode_format.lint(path)
            return result, output.getvalue()

    def test_valid_ordered_process_and_objects_pass(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace(
            "| SB1 | clip 1, frame f_001 | holds the product beside the symptom | labeled product bottle |",
            "| SB1 | clip 1, frames f_001-f_004 | tips saffron into oil in his palm | saffron capsule; oil bottle |\n"
            "| SB2 | clip 2, frame f_005 | mixes the ingredients into a paste | paste in palm |\n"
            "| SB3 | clip 2, frames f_006-f_009 | rubs the paste onto the belly | paste; belly |",
        )
        result, output = self.lint_text(text)
        self.assertEqual(0, result, output)

    def test_missing_shown_beats_ledger_fails(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace("### Shown beats ledger", "### Visual notes")
        result, output = self.lint_text(text)
        self.assertEqual(1, result)
        self.assertIn("missing `### Shown beats ledger`", output)

    def test_empty_shown_beats_ledger_fails(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace(
            "| SB1 | clip 1, frame f_001 | holds the product beside the symptom | labeled product bottle |",
            "",
        )
        result, output = self.lint_text(text)
        self.assertEqual(1, result)
        self.assertIn("shown-beats ledger has no four-column data row", output)

    def test_out_of_order_ids_fail(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace("| SB1 |", "| SB2 |")
        result, output = self.lint_text(text)
        self.assertEqual(1, result)
        self.assertIn("ordered SB1, SB2, SB3", output)

    def test_missing_source_evidence_fails(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace("clip 1, frame f_001", "opening shot")
        result, output = self.lint_text(text)
        self.assertEqual(1, result)
        self.assertIn("must name a source clip or frame", output)

    def test_explicit_no_shown_process_or_object_passes(self):
        text = decode_with_ledger("| none observed | n/a | n/a | n/a |")
        text = text.replace(
            "| SB1 | clip 1, frame f_001 | holds the product beside the symptom | labeled product bottle |",
            "| none observed | n/a | n/a | n/a |",
        )
        result, output = self.lint_text(text)
        self.assertEqual(0, result, output)


class CompositeGeometryTests(unittest.TestCase):
    """v938.3 — a foreground composite layer must carry MEASURED geometry.

    The corpus spent a year recording "small PiP bottom-left"; when the edit
    layer needed the box, every frame had to be re-measured. The checker cannot
    tell whether a box is right, so it requires the declaration AND requires it
    to contain numbers — an author made to write a fraction has to go measure.
    """

    FOREGROUND = (
        "### Image 2\n"
        "- **composite_layer:** foreground-keyed\n"
        "- **composite_with:** image_1\n"
        "- **layer_evidence:** hard matte edge, flat frontal light unlike the plate\n"
        "{geometry}"
        "\n"
    )

    def lint_text(self, text):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "decoded_fixture.md"
            path.write_text(text, encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = verify_decode_format.lint(path)
            return result, output.getvalue()

    def build(self, geometry, created="2026-08-22"):
        text = decode_with_ledger(
            "| enormous belly / adult man | fills the lower two-thirds of the frame "
            "and dwarfs his chest | 5/5 viral-max | NO |")
        text = f"created: {created}\n" + text
        return text.replace(
            "## Images\n### Image 1",
            "## Images\n### Image 1\n- **composite_layer:** background-plate\n"
            "- **composite_with:** image_2\n"
            "- **layer_evidence:** unbroken background, never occluded by the matte\n\n"
            + self.FOREGROUND.format(geometry=geometry))

    def test_measured_geometry_passes(self):
        geo = ("- **composite_geometry:** box: x[0..0.43W] y[0.57H..H] | flush: left, bottom | "
               "window: 0-2.5s (hook only) | plate: sharp full-frame\n")
        result, output = self.lint_text(self.build(geo))
        self.assertNotIn("v938.3", output, output)

    def test_missing_geometry_fails_in_the_new_era(self):
        result, output = self.lint_text(self.build(""))
        self.assertIn("v938.3", output)
        self.assertIn("FAIL", output)
        self.assertEqual(1, result, output)

    def test_adjectives_instead_of_numbers_fail(self):
        geo = "- **composite_geometry:** small inset, bottom-left corner\n"
        result, output = self.lint_text(self.build(geo))
        self.assertIn("v938.3", output)
        self.assertIn("almost no numbers", output)

    def test_older_decode_only_warns(self):
        """Forward-only: decodes written before the rule keep their prose."""
        result, output = self.lint_text(self.build("", created="2026-07-01"))
        self.assertIn("v938.3", output)
        self.assertIn("WARN", output)
        self.assertNotIn("FAIL: v938.3", output)

    def test_background_plate_needs_no_geometry(self):
        """The plate is full-frame by definition; only the layer in front has a box."""
        text = decode_with_ledger(
            "| enormous belly / adult man | fills the lower two-thirds of the frame "
            "and dwarfs his chest | 5/5 viral-max | NO |")
        text = "created: 2026-08-22\n" + text
        text = text.replace(
            "## Images\n### Image 1",
            "## Images\n### Image 1\n- **composite_layer:** background-plate\n"
            "- **composite_with:** image_2\n"
            "- **layer_evidence:** unbroken background, never occluded by the matte\n\n"
            + self.FOREGROUND.format(
                geometry="- **composite_geometry:** box: x[0..0.40W] y[0.67H..H] | "
                         "flush: left, bottom | window: 0-9s\n"))
        result, output = self.lint_text(text)
        self.assertNotIn("v938.3", output, output)


if __name__ == "__main__":
    unittest.main()
