import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "v589_video_understanding.py"
SPEC = importlib.util.spec_from_file_location("v589_video_understanding", MODULE_PATH)
v589 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v589)


def valid_stage4d():
    return {
        "schema_version": "stage4d.v2",
        "observed_people": [
            {"label": "person_1", "identity_markers": "adult, dark hair", "wardrobe": "blue shirt", "shots_present": [1]},
        ],
        "shots": [{
            "shot_index": 1,
            "start": 0.0,
            "end": 2.5,
            "summary": "The speaker lifts a glass while explaining the first step.",
            "forensic_perception": {
                "kinematic_traces": [{
                    "body_part_or_object": "right hand", "pixel_path": "fingers to wrist to blue sleeve",
                    "origin_or_owner": "person_1", "evidence": "continuous blue sleeve",
                }],
                "z_depth_layers": {"foreground": "glass", "midground": "person_1", "background": "kitchen"},
                "literal_vfx_observations": [],
                "intrinsic_state_isolation": {
                    "focus_object": "glass", "axis_1_surface_texture": "unchanged",
                    "axis_2_structural_integrity": "unchanged", "axis_3_volume_shape": "unchanged",
                    "axis_4_color_illumination": "unchanged", "primary_change_axis": "NONE",
                    "intrinsic_state_start": "clear empty glass", "intrinsic_state_end": "clear empty glass",
                },
            },
            # frame_inventory + start_frame_spec are required by PER_SHOT_SCHEMA.
            # This fixture predates 2026-08-12 and omitted both: validate_stage4d_output
            # used a hand-typed required list that had drifted from the schema and did
            # not check them. That list now derives from PER_SHOT_SCHEMA, so the fixture
            # has to be what it always claimed to be — a complete stage4d.v2 record.
            "frame_inventory": {
                "set_and_background": "Domestic kitchen; pale grey counter, white subway tile, window at frame right.",
                "wardrobe_and_grooming": "person_1 in a mid-blue cotton shirt, sleeves to the wrist, short dark hair.",
                "props_and_objects": "Clear empty drinking glass, person_1's own, held in the right hand on the subject layer.",
                "on_screen_text_verbatim": "none",
                "color_palette": "Warm neutrals: cream tile, pale grey stone, mid-blue shirt.",
                "production_method": "Single continuous live-action take; no compositing tells.",
            },
            "start_frame_spec": {
                "camera_height_and_angle": "Level with the eyes, straight-on.",
                "camera_distance_and_lens": "About 1.5 m, neutral ~40mm; ear-to-nose relationship is undistorted.",
                "subject_placement_and_crop": "Head centred, a palm of headroom, crop at mid-chest below the second shirt button.",
                "background_depth_and_defocus": "Counter about 1 m behind, softly blurred; tile grout still readable.",
                "lighting_setup": "Single soft window key from frame right, no fill, gentle falloff to frame left.",
                "color_temp_and_grade": "Warm daylight near 5200K, neutral grade, no lift in the blacks.",
                "texture_and_artifacts": "Clean digital capture, fine grain, no visible compression blocking.",
                "graphics_geometry": "none",
                "posture_and_gaze": "Squared to camera, gaze into the lens, right hand rising into frame.",
                "recreation_variable_slots": "Swappable: shirt colour, glass contents, window light direction.",
            },
            "static_composition": {
                "subject": "person_1 faces camera and raises one hand", "framing": "face and glass details are sharp",
                "anchor_props_with_positions": "glass held beside person_1", "lighting_and_palette": "warm window light",
            },
            "action_arc": {
                "has_state_evolution": False,
                "kinematics": {"start_state": "glass on counter", "mid_state": "hand grips glass", "end_state": "glass raised", "movement_path": "counter to chest height", "camera_shift": "static"},
                "morphology": {"focus_object": "glass", "intrinsic_state_start": "clear empty glass", "intrinsic_state_end": "clear empty glass", "primary_change_axis": "NONE", "magnitude": "NONE"},
                "verbs_observed": ["lifts"],
            },
            "audio": {"dialogue_verbatim": "First, lift the glass.", "ambient": "room tone", "music": "none", "voice_register": "direct"},
            "motion_cross_check": {"input_classification": "medium", "vlm_observation": "one hand lift", "agrees": True, "discrepancy": "none"},
            "veo_reproduction_hints": {"use_blend_to_next_scene": False, "needs_platform_future_image_end": False, "transition_prompt": "Lift the glass from counter to chest height."},
            "human_walk_corrections": "none",
        }],
    }


class Stage4dContractTests(unittest.TestCase):
    def setUp(self):
        self.expected = [{"shot": 1, "start": 0.0, "end": 2.5}]

    def test_valid_contract_passes(self):
        result = v589.validate_stage4d_output(valid_stage4d(), self.expected)
        self.assertEqual(result["schema_version"], "stage4d.v2")

    def test_old_array_contract_fails(self):
        with self.assertRaisesRegex(v589.Stage4dValidationError, "top level"):
            v589.validate_stage4d_output(valid_stage4d()["shots"], self.expected)

    def test_missing_forensic_block_fails(self):
        data = valid_stage4d()
        del data["shots"][0]["forensic_perception"]
        with self.assertRaisesRegex(v589.Stage4dValidationError, "forensic_perception"):
            v589.validate_stage4d_output(data, self.expected)

    def test_source_timestamp_mismatch_fails(self):
        data = valid_stage4d()
        data["shots"][0]["end"] = 3.0
        with self.assertRaisesRegex(v589.Stage4dValidationError, "end must match"):
            v589.validate_stage4d_output(data, self.expected)

    def test_changed_morphology_needs_distinct_states(self):
        data = valid_stage4d()
        morphology = data["shots"][0]["action_arc"]["morphology"]
        morphology["primary_change_axis"] = "Color/Illumination"
        with self.assertRaisesRegex(v589.Stage4dValidationError, "distinct start/end"):
            v589.validate_stage4d_output(data, self.expected)

    def test_gemini_prompt_uses_motion_without_repeating_schema(self):
        prompt = v589.build_user_prompt(self.expected, "hello", {"shots": [{"shot": 1, "motion": "medium"}]}, include_schema=False)
        self.assertIn("Motion classifications", prompt)
        self.assertNotIn('"additionalProperties"', prompt)
        self.assertIn("JSON OBJECT", prompt)

    def test_standard_cost_is_only_an_estimate(self):
        estimate = v589.compute_gemini_cost("gemini-3.6-flash", 1000, 100, 200)
        self.assertAlmostEqual(estimate, 0.003)

    def test_human_template_is_shaped_but_not_handoff_ready(self):
        template = json.loads(v589.write_human_walk_template(self.expected, {"segments": []}, None))
        self.assertEqual(template["schema_version"], "stage4d.v2")
        # Pin the SHAPE, not just the error. Asserting only that validation
        # raises "placeholders" is not enough: when the template was missing
        # frame_inventory + start_frame_spec it also collected two
        # missing-field errors per shot, still matched the regex, and stayed
        # green while the generator emitted unvalidatable artifacts. The
        # template must be structurally complete — only placeholders may block it.
        self.assertEqual(set(template["shots"][0]), set(v589.PER_SHOT_SCHEMA["required"]))
        with self.assertRaisesRegex(v589.Stage4dValidationError, "placeholders"):
            v589.validate_stage4d_output(template, self.expected)

    def test_filled_in_human_template_reaches_handoff(self):
        """The lane prints 'fill placeholders, then run --validate-stage4d'.
        Walk that instruction end to end: every placeholder replaced must
        validate. This is what a missing schema field silently makes impossible."""
        template = json.loads(v589.write_human_walk_template(self.expected, {"segments": []}, None))

        def fill(node):
            if isinstance(node, dict):
                return {k: fill(v) for k, v in node.items()}
            if isinstance(node, list):
                return [fill(v) for v in node]
            if isinstance(node, str) and ("<REQUIRED" in node or "<COPY FROM" in node):
                return "observed by the human walk"
            return node

        filled = fill(template)
        result = v589.validate_stage4d_output(filled, self.expected)
        self.assertEqual(result["schema_version"], "stage4d.v2")

    def test_parse_and_validate_is_scope_safe(self):
        # Regression: this function once referenced main()'s locals
        # (provider_used / out) and raised NameError on every provider call.
        parsed = v589.parse_and_validate_stage4d(json.dumps(valid_stage4d()), self.expected)
        self.assertEqual(parsed["schema_version"], "stage4d.v2")
        with self.assertRaisesRegex(v589.Stage4dValidationError, "invalid JSON"):
            v589.parse_and_validate_stage4d("not json {", self.expected)

    def test_shots_reader_accepts_tsv_and_json_identically(self):
        """One reader, one rule. --validate-stage4d used to run a bare
        json.loads and die on the clips.tsv the decode path accepts."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            tsv = tmp / "clips.tsv"
            tsv.write_text(
                "clip\tstart_sec\tstart_frame\tend_sec\tdur_s\timage\tsame_as\n"
                "1\t0.000\t0\t2.500\t2.500\timage_1\t\n",
                encoding="utf-8")
            js = tmp / "shots.json"
            js.write_text(json.dumps([{"shot": 1, "start": 0.0, "end": 2.5}]), encoding="utf-8")

            from_tsv = v589._load_shots_file(tsv)
            from_json = v589._load_shots_file(js)
            for key in ("shot", "start", "end"):
                self.assertEqual(from_tsv[0][key], from_json[0][key], key)
            # a shots.json written by an older converter still normalizes
            old = tmp / "old.json"
            old.write_text(json.dumps([{"shot_index": 1, "start": 0.0, "end": 2.5}]), encoding="utf-8")
            self.assertEqual(v589._load_shots_file(old)[0]["shot"], 1)

    def test_validate_cli_accepts_a_clips_tsv(self):
        """End to end through main(): the invocation printed in the plan and the
        decode skill must work, not raise JSONDecodeError."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "clips.tsv").write_text(
                "clip\tstart_sec\tstart_frame\tend_sec\tdur_s\timage\tsame_as\n"
                "1\t0.000\t0\t2.500\t2.500\timage_1\t\n",
                encoding="utf-8")
            (tmp / "artifact.json").write_text(
                json.dumps(valid_stage4d()), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(MODULE_PATH),
                 "--validate-stage4d", str(tmp / "artifact.json"),
                 "--shots", str(tmp / "clips.tsv")],
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("PASS", result.stdout)

            # and a genuinely malformed shots file still says something useful
            (tmp / "broken.json").write_text("{not json", encoding="utf-8")
            broken = subprocess.run(
                [sys.executable, str(MODULE_PATH),
                 "--validate-stage4d", str(tmp / "artifact.json"),
                 "--shots", str(tmp / "broken.json")],
                capture_output=True, text=True,
            )
            self.assertEqual(broken.returncode, 2)
            self.assertIn("could not read shots", broken.stderr)

    def test_help_is_windows_console_safe(self):
        result = subprocess.run(
            [sys.executable, str(MODULE_PATH), "--help"],
            capture_output=True, text=True, encoding="ascii", errors="strict",
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
