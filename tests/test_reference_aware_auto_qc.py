"""Focused regressions for the v964 reference-aware auto-choice boundary."""
import copy
import hashlib
import json

import pytest

from image_qc_contract import (
    MAX_SNAPSHOT_BYTES,
    RUBRIC_VERSION,
    build_input_snapshot,
    canonical_reference_intent,
    snapshot_digest,
    snapshot_size,
    validate_auto_decision,
)


def _node(intents=("product",), multi_person=False):
    edges = []
    for i, intent in enumerate(intents, 1):
        edges.append({
            "id": i, "parent_node_id": 100 + i, "role": intent,
            "kind": "product" if intent == "product" else
                    "character" if intent == "identity" else "chain",
            "reference_class": "product" if intent == "product" else
                               "character" if intent == "identity" else "chain",
            "reference_intent": intent, "origin": "manual",
            "parent_origin": "manual", "parent_chosen_variant_id": 900 + i,
            "parent_status": "ready",
            "reference_instruction": "" if intent in {"product", "identity"} else "keep this exact body",
        })
    return {
        "id": 1, "kind": "generated", "origin": "manual", "status": "ready",
        "chosen_variant_id": None, "prompt": "one adult in a kitchen",
        "aspect_ratio": "9:16", "resolution": "2K", "model": "nano_banana_2",
        "n_variants": 2, "action_note": None, "frame_anchor_s": None,
        "visual_delta": None, "narrative_lens": None, "cast": ["Nuri"],
        "role": None, "pair_role": None, "paired_with_image_node_id": None,
        "batch_id": "b", "scene_index_in_batch": 0,
        "multi_person": multi_person,
        "variants": [{"id": 11, "source": "ai"}, {"id": 12, "source": "ai"}],
        "parents": edges,
    }


def _report(node, face=(0.5, 0.5), *, bad=None, snapshot=None):
    candidates = node["variants"]
    parent_rows = [{"id": e["parent_node_id"], "status": "ready",
                    "chosen_variant_id": e["parent_chosen_variant_id"],
                    "origin": "manual"} for e in node["parents"]]
    candidate_bytes = {v["id"]: bytes([v["id"] % 255]) for v in candidates}
    parent_bytes = {p["chosen_variant_id"]: b"parent" for p in parent_rows}
    snap = snapshot or build_input_snapshot(
        node, candidates=candidates, edges=node["parents"], parents=parent_rows,
        candidate_bytes=candidate_bytes, parent_bytes=parent_bytes,
    )
    digest = snapshot_digest(snap)
    def row(vid, status="pass"):
        return {
            "variant_id": vid, "person_count": "one", "prompt_status": status,
            "required_elements_visible": True, "element_misses": [],
            "warnings": [], "hard_fail_codes": [],
            "references": [{
                "edge_id": e["id"],
                "parent_variant_id": e["parent_chosen_variant_id"],
                "status": "pass", "observable": True, "fail_codes": [],
                "observed_hero_text": "KORELLA",
            } for e in node["parents"]],
        }
    rows = [row(v["id"]) for v in candidates]
    if bad is not None:
        rows[bad]["element_misses"] = ["missing declared object"]
    reads = [{"read_id": i, "snapshot_digest": digest,
              "candidates": copy.deepcopy(rows)} for i in (1, 2)]
    face_values = list(face)
    if face_values:
        face_values.extend([face_values[-1]] * (len(candidates) - len(face_values)))
    return {
        "version": 1, "auto_contract_version": 1,
        "rubric_version": RUBRIC_VERSION, "input_snapshot": snap,
        "snapshot_digest": digest,
        "reference_face_sim": {str(v["id"]): face_values[i]
                                for i, v in enumerate(candidates)},
        "variants": {str(v["id"]): {"integrity": {"ok": True}}
                     for v in candidates},
        "reads": reads,
    }


def test_product_only_can_choose_without_face_requirement():
    n = _node(("product",))
    r = _report(n, bad=1)
    r.pop("reference_face_sim")
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "choose"


def test_decide_auto_choice_accepts_current_strict_report():
    import image_qc
    n = _node(("product",))
    r = _report(n, bad=1)
    r.pop("reference_face_sim")
    out = image_qc.decide_auto_choice(n, r)
    assert out["decision"] == "choose" and out["variant_id"] == 11


def test_old_rubric_report_is_review():
    n = _node(("product",))
    r = _report(n, bad=1)
    r["rubric_version"] = "image-reference-qc-v0"
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_oversized_snapshot_is_review_without_truncating_fingerprints():
    n = _node(("product",))
    n["prompt"] = "é" * 7000
    n["action_note"] = "action " + "x" * 5000
    n["visual_delta"] = "delta " + "y" * 5000
    n["cast"] = ["cast " + "z" * 5000]
    n["parents"][0]["reference_instruction"] = "instruction " + "q" * 5000
    snap = build_input_snapshot(
        n, candidates=n["variants"], edges=n["parents"],
        parents=[{"id": 101, "status": "ready", "chosen_variant_id": 901,
                  "origin": "manual"}],
        candidate_bytes={11: b"a", 12: b"b"}, parent_bytes={901: b"p"})
    assert snapshot_size(snap) > MAX_SNAPSHOT_BYTES
    r = _report(n, snapshot=snap, bad=1)
    assert validate_auto_decision(n, r, current_snapshot=snap)["decision"] == "review"


def test_stored_snapshot_digest_must_bind_and_current_snapshot_must_fit():
    n = _node(("product",))
    r = _report(n, bad=1)
    current = copy.deepcopy(r["input_snapshot"])
    current["child"]["prompt"] = "x" * (MAX_SNAPSHOT_BYTES + 1)
    # The report's stored snapshot is small and internally valid, but the
    # server-side current snapshot is oversized and must not qualify.
    assert snapshot_size(r["input_snapshot"]) <= MAX_SNAPSHOT_BYTES
    assert validate_auto_decision(n, r, current_snapshot=current)["decision"] == "review"

    broken = copy.deepcopy(r)
    broken["input_snapshot"]["child"]["prompt"] = "changed"
    assert validate_auto_decision(n, broken,
                                  current_snapshot=r["input_snapshot"])["decision"] == "review"


@pytest.mark.parametrize("status, chosen", [("draft", 901), ("ready", None)])
def test_unready_or_unchosen_parent_is_review(status, chosen):
    n = _node(("product",))
    n["parents"][0]["parent_status"] = status
    n["parents"][0]["parent_chosen_variant_id"] = chosen
    r = _report(n, bad=1)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_character_and_product_can_choose():
    n = _node(("identity", "product"))
    r = _report(n, bad=1)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "choose"


def test_one_bad_face_is_removed_not_whole_node():
    n = _node(("identity",))
    r = _report(n, face=(0.2, 0.6))
    out = validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])
    assert out["decision"] == "choose" and out["variant_id"] == 12


@pytest.mark.parametrize("intent", ["continuity", "body", "support", "role"])
def test_manual_intent_makes_whole_node_manual(intent):
    n = _node(("product", intent))
    r = _report(n)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_no_reference_and_multi_person_stay_manual():
    n = _node(())
    assert validate_auto_decision(n, None)["decision"] == "review"


@pytest.mark.parametrize("prompt", [
    "a woman with her husband",
    "husband and wife in a kitchen",
    "a man beside a woman",
    "one adult with another person",
    "a couple in a kitchen",
])
def test_real_multi_person_prompt_phrases_stay_manual(prompt):
    n = _node(("product",))
    n["prompt"] = prompt
    r = _report(n, bad=1)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_product_blank_hero_evidence_is_manual():
    n = _node(("product",))
    r = _report(n, bad=1)
    for read in r["reads"]:
        read["candidates"][0]["references"][0]["observed_hero_text"] = ""
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


@pytest.mark.parametrize("count", ["multiple", "unknown"])
def test_multiple_or_unclear_person_count_is_manual(count):
    n = _node(("product",))
    r = _report(n, bad=1)
    for read in r["reads"]:
        read["candidates"][0]["person_count"] = count
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_person_count_reads_must_agree():
    n = _node(("product",))
    r = _report(n, bad=1)
    r["reads"][1]["candidates"][0]["person_count"] = "zero"
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_product_zero_person_count_can_choose():
    n = _node(("product",))
    n["prompt"] = "a product jar on a counter"
    r = _report(n, bad=1)
    for read in r["reads"]:
        read["candidates"][0]["person_count"] = "zero"
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "choose"


def test_identity_reference_requires_one_person():
    n = _node(("identity",))
    r = _report(n, bad=1)
    for read in r["reads"]:
        read["candidates"][0]["person_count"] = "zero"
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


@pytest.mark.parametrize("code", [
    "brand_text_mismatch", "dosage_text_mismatch", "hero_text_unreadable",
])
def test_product_wrong_or_unreadable_evidence_is_manual(code):
    n = _node(("product",))
    r = _report(n, bad=1)
    for read in r["reads"]:
        read["candidates"][0]["references"][0]["fail_codes"] = [code]
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


@pytest.mark.parametrize("value", [{"text": "KORELLA"}, ["KORELLA"], 7])
def test_validator_rejects_non_string_hero_evidence(value):
    n = _node(("product",))
    r = _report(n, bad=1)
    r["reads"][0]["candidates"][0]["references"][0]["observed_hero_text"] = value
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


@pytest.mark.parametrize("code", ["different_person", "face_not_observable"])
def test_identity_fail_codes_are_supported_and_block(code):
    from image_qc import parse_reference_judge_reply
    edge = [{"edge_id": 1, "intent": "identity", "parent_variant_id": 901}]
    raw = json.loads(_raw_read())
    for candidate in raw["candidates"]:
        candidate["references"][0]["fail_codes"] = [code]
    parsed = parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64)
    assert parsed is not None
    n = _node(("product",), multi_person=True)
    assert validate_auto_decision(n, None)["decision"] == "review"


def test_integrity_and_element_miss_block_survivor():
    n = _node(("product",))
    r = _report(n, bad=0)
    r["variants"]["11"]["integrity"]["ok"] = False
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "choose"
    r2 = _report(n, bad=1)
    assert validate_auto_decision(n, r2, current_snapshot=r2["input_snapshot"])["decision"] == "choose"


def test_two_good_candidates_require_manual_review():
    n = _node(("product",))
    r = _report(n)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_snapshot_parent_hash_uses_variant_id_and_changes_with_bytes():
    n = _node(("product",))
    parents = [{"id": 101, "status": "ready", "chosen_variant_id": 901,
                "origin": "manual"}]
    a = build_input_snapshot(n, parents=parents, candidate_bytes={11: b"a", 12: b"b"}, parent_bytes={901: b"one"})
    b = build_input_snapshot(n, parents=parents, candidate_bytes={11: b"a", 12: b"b"}, parent_bytes={901: b"two"})
    assert a["parents"][0]["chosen_image_sha256"] == hashlib.sha256(b"one").hexdigest()
    assert a["parents"][0]["chosen_image_sha256"] != b["parents"][0]["chosen_image_sha256"]


def test_stale_prompt_and_parent_choice_force_review():
    n = _node(("product",))
    r = _report(n, bad=1)
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "choose"
    changed = copy.deepcopy(n); changed["prompt"] = "different prompt"
    assert validate_auto_decision(changed, r, current_snapshot=build_input_snapshot(changed, candidates=changed["variants"], edges=changed["parents"], parents=r["input_snapshot"]["parents"], candidate_bytes={11: bytes([11]), 12: bytes([12])}, parent_bytes={901: b"parent"}))["decision"] == "review"
    parent_changed = copy.deepcopy(n)
    parent_changed["parents"][0]["parent_chosen_variant_id"] = 902
    parent_rows = [{"id": 101, "status": "ready", "chosen_variant_id": 902, "origin": "manual"}]
    current = build_input_snapshot(parent_changed, candidates=parent_changed["variants"], edges=parent_changed["parents"], parents=parent_rows, candidate_bytes={11: bytes([11]), 12: bytes([12])}, parent_bytes={902: b"parent"})
    assert validate_auto_decision(parent_changed, r, current_snapshot=current)["decision"] == "review"
    current = build_input_snapshot(n, candidates=n["variants"], edges=n["parents"], parents=r["input_snapshot"]["parents"], candidate_bytes={11: bytes([11]), 12: bytes([12])}, parent_bytes={901: b"changed"})
    assert validate_auto_decision(n, r, current_snapshot=current)["decision"] == "review"


def test_legacy_report_without_integrity_is_manual():
    n = _node(("product",)); r = _report(n)
    r.pop("variants")
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_digest_changes_when_candidate_bytes_change():
    n = _node(("product",)); parents = [{"id": 101, "status": "ready", "chosen_variant_id": 901, "origin": "manual"}]
    a = build_input_snapshot(n, parents=parents, candidate_bytes={11: b"a", 12: b"b"}, parent_bytes={901: b"p"})
    b = build_input_snapshot(n, parents=parents, candidate_bytes={11: b"changed", 12: b"b"}, parent_bytes={901: b"p"})
    assert snapshot_digest(a) != snapshot_digest(b)


def test_only_explicit_chain_role_is_continuity():
    assert canonical_reference_intent({"kind": "chain", "role": "reference"}) == "role"
    assert canonical_reference_intent({"kind": "chain", "role": "chain_from_image_1"}) == "continuity"


def test_reference_prompt_has_closed_rubric_and_trusted_labels():
    from image_qc import build_reference_judge_prompt
    prompt = build_reference_judge_prompt(
        "one adult holding the product",
        [{"edge_id": 7, "parent_variant_id": 901, "intent": "product",
          "role": "product reference",
          "reference_instruction": 'show the exact brand "KORELLA"; preserve café label'}],
        [11],
        "swap the jar for a blue tin",
    )
    assert "parent_variant_id=901" in prompt
    assert "show the exact brand" in prompt
    assert "\\u00e9" in prompt
    assert "VISUAL DELTA (DATA)" in prompt
    assert "person_count (zero|one|multiple|unknown)" in prompt
    assert "swap the jar for a blue tin" in prompt
    assert "brand_text_mismatch" in prompt
    assert "Minor cap, color, layout" in prompt
    example = prompt[prompt.rfind('{"candidates"'):]
    parsed = json.loads(example)
    from image_qc import _REFERENCE_CANDIDATE_KEYS
    assert set(parsed["candidates"][0]) == set(_REFERENCE_CANDIDATE_KEYS)


def _raw_read(edge_parent=901, candidate_ids=(11, 12)):
    return json.dumps({"candidates": [{
        "variant_id": vid, "person_count": "one", "prompt_status": "pass",
        "required_elements_visible": True, "element_misses": [],
        "warnings": [], "hard_fail_codes": [], "references": [{
            "edge_id": 1, "parent_variant_id": edge_parent,
            "status": "pass", "observable": True, "fail_codes": [],
            "observed_hero_text": "KORELLA",
        }],
    } for vid in candidate_ids]})


def test_parser_rejects_wrong_parent_and_duplicate_or_extra_ids():
    from image_qc import parse_reference_judge_reply
    edge = [{"edge_id": 1, "intent": "product", "parent_variant_id": 901}]
    assert parse_reference_judge_reply(_raw_read(902), [11, 12], edge, 1, "a" * 64) is None
    assert parse_reference_judge_reply(_raw_read(candidate_ids=(11, 11)), [11, 12], edge, 1, "a" * 64) is None
    assert parse_reference_judge_reply(_raw_read(candidate_ids=(11, 12, 13)), [11, 12], edge, 1, "a" * 64) is None
    assert parse_reference_judge_reply(_raw_read(), [11, 12],
                                      [{"edge_id": 1, "intent": "product",
                                        "parent_variant_id": "not-an-id"}],
                                      1, "a" * 64) is None


def test_parser_rejects_unknown_status_and_duplicate_or_extra_reference_edges():
    from image_qc import parse_reference_judge_reply
    edge = [{"edge_id": 1, "intent": "product", "parent_variant_id": 901}]
    raw = json.loads(_raw_read())
    raw["candidates"][0]["references"][0]["status"] = "unknownish"
    assert parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64) is None
    raw = json.loads(_raw_read())
    raw["candidates"][0]["references"].append(copy.deepcopy(raw["candidates"][0]["references"][0]))
    assert parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64) is None
    raw = json.loads(_raw_read())
    raw["candidates"][0]["references"][0]["edge_id"] = 2
    assert parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64) is None


@pytest.mark.parametrize("field,value", [
    ("element_misses", None), ("element_misses", "missing"),
    ("element_misses", {"bad": "shape"}),
    ("element_misses", ["x" * 241]),
    ("warnings", None), ("warnings", "warning"),
    ("warnings", {"bad": "shape"}), ("warnings", ["x" * 241]),
])
def test_parser_rejects_non_bounded_reference_lists(field, value):
    from image_qc import parse_reference_judge_reply
    edge = [{"edge_id": 1, "intent": "product", "parent_variant_id": 901}]
    raw = json.loads(_raw_read())
    for candidate in raw["candidates"]:
        candidate[field] = value
    assert parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64) is None


@pytest.mark.parametrize("field,value", [
    ("hard_fail_codes", {"bad": "shape"}),
    ("hard_fail_codes", ["compliance", {"bad": "shape"}]),
    ("references.fail_codes", {"bad": "shape"}),
    ("references.fail_codes", ["product_missing", {"bad": "shape"}]),
    ("references.observed_hero_text", None),
    ("references.observed_hero_text", {"not": "text"}),
    ("references.observed_hero_text", "x" * 241),
])
def test_parser_rejects_malformed_hard_codes_and_reference_evidence(field, value):
    from image_qc import parse_reference_judge_reply
    edge = [{"edge_id": 1, "intent": "product", "parent_variant_id": 901}]
    raw = json.loads(_raw_read())
    target = raw["candidates"][0]
    if field == "hard_fail_codes":
        target[field] = value
    else:
        target["references"][0][field.split(".", 1)[1]] = value
    assert parse_reference_judge_reply(json.dumps(raw), [11, 12], edge, 1, "a" * 64) is None


def test_both_complete_reads_are_checked_for_bounded_lists():
    n = _node(("product",))
    r = _report(n, bad=1)
    r["reads"][1]["candidates"][0]["warnings"] = {"not": "a list"}
    assert validate_auto_decision(n, r, current_snapshot=r["input_snapshot"])["decision"] == "review"


def test_worst_case_contract_report_fits_after_reference_free_text_trim():
    from image_qc import fit_report, _report_size
    n = _node(("product",) * 8)
    n["variants"] = [{"id": 11 + i, "source": "ai"} for i in range(4)]
    r = _report(n)
    for read in r["reads"]:
        for row in read["candidates"]:
            row["warnings"] = ["é" * 240] * 8
            row["element_misses"] = ["é" * 240] * 8
            for ref in row["references"]:
                ref["observed_hero_text"] = "é" * 240
    fitted = fit_report(r)
    assert _report_size(fitted) <= 60_000
