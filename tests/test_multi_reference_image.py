"""v859 — image-block parser accepts ONE or TWO chain parents.

    - **reference_image:** image_3            -> [3]
    - **reference_image:** image_3, image_2   -> [3, 2]

Backward compat is the load-bearing constraint: the legacy scalar key
``reference_image`` keeps meaning exactly what it meant pre-v859 (= the
FIRST entry, or None), because ~10 downstream call sites read it. The
full list rides on the NEW key ``reference_images``.

The markdown fixture is built with .replace() rather than an f-string:
the fixture embeds triple-backtick fences, and keeping it a plain
(non-f) string avoids brace/fence quoting fights entirely.
"""

import pytest

from image_platform import (
    _parse_image_blocks_new,
    _parse_scene_blocks_legacy,
    _v619_n5_drop_invalid_chain_refs,
    _v859_plan_chain_edges,
)


_MD_TEMPLATE = """## Images

### Image 1
- **reference_image:** none
- **Image prompt:**
```
A photo of a kitchen.
Aspect ratio 9:16.
```

### Image 2
- **reference_image:** image_1
- **Image prompt:**
```
The same kitchen, wider.
Aspect ratio 9:16.
```

### Image 3
__REF_LINE__
- **Image prompt:**
```
The same kitchen at night.
Aspect ratio 9:16.
```
"""


def _md(ref_line):
    return _MD_TEMPLATE.replace("__REF_LINE__", ref_line)


def _md4(ref_line, on_image=3):
    """4-image doc so a TRUE forward ref (ref > index) is expressible.

    _md puts the ref on the highest image, which makes image_N a SELF ref
    (==) and image_N+1 fail the existence check first — so the `>` half of
    `ref >= image_index` is unreachable there. This fixture exists purely to
    reach it.
    """
    blocks = []
    for n in (1, 2, 3, 4):
        ref = ref_line if n == on_image else (
            "- **reference_image:** none" if n == 1 else "- **reference_image:** image_1"
        )
        blocks.append(
            f"### Image {n}\n{ref}\n- **Image prompt:**\n"
            "```\nA photo of a kitchen.\nAspect ratio 9:16.\n```\n"
        )
    return "## Images\n\n" + "\n".join(blocks)


def _third(ref_line):
    imgs = _parse_image_blocks_new(_md(ref_line))
    return [i for i in imgs if i["image_index"] == 3][0]


def test_single_ref_unchanged():
    third = _third("- **reference_image:** image_1")
    assert third["reference_image"] == 1
    assert third["reference_images"] == [1]


def test_two_refs_parsed_in_order():
    third = _third("- **reference_image:** image_2, image_1")
    assert third["reference_images"] == [2, 1]
    assert third["reference_image"] == 2   # legacy key = first entry


def test_none_gives_empty_list():
    third = _third("- **reference_image:** none")
    assert third["reference_image"] is None
    assert third["reference_images"] == []


def test_three_refs_rejected():
    with pytest.raises(ValueError, match="at most 2"):
        _parse_image_blocks_new(_md("- **reference_image:** image_2, image_1, image_2"))


# --- Backward-compat: DECORATED values (trailing author notes) -------------
# The pre-v859 regex captured a single \S+ token, so a trailing note was
# simply never seen: "none (location shift)" -> "none" -> None. v859 widened
# the capture to reach a comma list, so it must re-drop the note explicitly
# or it hard-fails a real input class that used to work. 10 decode docs in
# raw/videos/ carry exactly this shape.

def test_decorated_none_is_tolerated_like_pre_v859():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** none (location shift kitchen to office)"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] is None
    assert third["reference_images"] == []


def test_decorated_ref_is_tolerated_like_pre_v859():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** image_1 (keep the counter)"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] == 1
    assert third["reference_images"] == [1]


def test_malformed_token_still_raises():
    with pytest.raises(ValueError, match="bad reference_image token"):
        _parse_image_blocks_new(_md("- **reference_image:** image_x"))


def test_duplicate_refs_rejected():
    # Exactly 2 entries, so the ">2" guard does NOT fire — this is the only
    # test that actually reaches the duplicate branch. Banana 2 down-weights
    # duplicate refs and it wastes a slot (the v520/v522 lesson).
    with pytest.raises(ValueError, match="duplicate reference_image entries"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_1"))


def test_blank_value_is_none_like_pre_v859():
    # Regression: the old capture bled onto the next line and raised
    # "bad reference_image token '-'". Blank must fall through to None.
    imgs = _parse_image_blocks_new(_md("- **reference_image:**"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] is None
    assert third["reference_images"] == []


# --- v859 validation: EVERY chain parent is checked, not just the first -----
# Pre-v859 only the scalar (= first entry) was validated, so a bad SECOND ref
# imported silently and blew up later at generation time.

def test_second_ref_unknown_is_rejected():
    with pytest.raises(ValueError, match="image_9 which doesn't exist"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_9"))


def test_second_ref_self_is_rejected():
    # _md puts the ref line on Image 3, the HIGHEST image in the fixture, so
    # "image_3" here is a SELF ref: this test covers the == case of
    # `ref >= image_index`. The > case (a TRUE forward ref) needs a doc with a
    # higher image to point at — see test_true_forward_ref_is_rejected.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_3"))


def test_true_forward_ref_is_rejected():
    # Reaches the `>` half of `ref >= image_index`: image_4 EXISTS here, so the
    # existence check passes and the forward check is what must fire.
    # Mutation check: this test dies if `>=` is weakened to `==`.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md4("- **reference_image:** image_1, image_4", on_image=3))


def test_true_forward_ref_rejected_single_legacy_ref():
    # Same branch via the LEGACY single-ref shape — proves v859 didn't only
    # protect the new list form.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md4("- **reference_image:** image_3", on_image=2))


def test_valid_two_refs_pass_validation():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** image_2, image_1"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_images"] == [2, 1]


# --- v859 scope guard: multi-ref is NEW-FORMAT only -------------------------
# The LEGACY scene parser is the other format: its scenes carry
# reference_image directly (the new format binds via "- **image:** image_N"
# and has no reference_image at all). Multi-ref is not ported there — but it
# must not half-apply either.

LEGACY_MULTIREF = '''### Scene 1
- **reference_image:** none
**Image prompt:**
```
A kitchen.
```

### Scene 2
- **reference_image:** image_1, image_1
**Image prompt:**
```
The same kitchen.
```
'''


def test_legacy_scene_parser_refuses_multiref():
    # Pre-v859 this silently captured "image_1," via an UNANCHORED regex -> 1,
    # dropping entry 2 with no error. Multi-ref is a new-format image-block
    # feature; refuse it here loudly rather than half-applying it.
    with pytest.raises(ValueError, match="multi-reference"):
        _parse_scene_blocks_legacy(LEGACY_MULTIREF)


# ===========================================================================
# v859 — chain-edge planning (_v859_plan_chain_edges)
# ===========================================================================
# The edge-creation loop lives inside _import_scene_table_impl, a ~1300-line
# FastAPI handler that needs a Session, an authenticated User, ready ImageNode
# uploads and a subject row before it reaches the chain block. The DECISION
# ("which edges, in which slots, which get skipped") is what v859 changes, so
# it is extracted into a pure planner and tested directly here. No DB, no
# logging, no mocks.


def test_plan_two_refs_makes_two_edges_in_declaration_order():
    # THE v859 feature: refs[0] = pose/held-objects parent, refs[1] = body
    # parent. Order is authoritative — the slot translator reads chain order.
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "node3", 2: "node2"}, attached_parents_count=1,
        bound_parent_ids={"persona"}, slot=1,
    )
    assert [e["ref"] for e in plan["edges"]] == [3, 2]
    assert [e["slot_order"] for e in plan["edges"]] == [1, 2]
    assert [e["parent_id"] for e in plan["edges"]] == ["node3", "node2"]
    assert plan["attached_parents_count"] == 3
    assert plan["slot"] == 3
    assert plan["capped"] is None and plan["missing"] is None


def test_plan_single_ref_unchanged_from_pre_v859():
    plan = _v859_plan_chain_edges(
        [1], {1: "node1"}, attached_parents_count=1,
        bound_parent_ids={"persona"}, slot=1,
    )
    assert [(e["ref"], e["slot_order"]) for e in plan["edges"]] == [(1, 1)]
    assert plan["attached_parents_count"] == 2


def test_plan_no_refs_makes_no_edges():
    plan = _v859_plan_chain_edges(
        [], {}, attached_parents_count=1, bound_parent_ids={"persona"}, slot=1,
    )
    assert plan["edges"] == []
    assert plan["attached_parents_count"] == 1
    assert plan["slot"] == 1


def test_plan_skips_parent_already_bound_via_ingredient():
    # v520/v522: Banana 2 down-weights a duplicate ref AND it wastes one of
    # only 3 slots. The duplicate must not consume a slot or the count.
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "node3", 2: "node2"}, attached_parents_count=1,
        bound_parent_ids={"node3"}, slot=1,
    )
    assert [e["ref"] for e in plan["edges"]] == [2]
    assert plan["duplicates"] == [3]
    assert plan["edges"][0]["slot_order"] == 1     # slot NOT burned by the dup
    assert plan["attached_parents_count"] == 2


def test_plan_two_refs_pointing_at_same_parent_dedupes_second():
    # Distinct ref indexes can still resolve to the same parent node.
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "same", 2: "same"}, attached_parents_count=0,
        bound_parent_ids=set(), slot=1,
    )
    assert [e["ref"] for e in plan["edges"]] == [3]
    assert plan["duplicates"] == [2]


def test_plan_respects_three_parent_cap_when_already_full():
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "node3", 2: "node2"}, attached_parents_count=3,
        bound_parent_ids={"a", "b", "c"}, slot=3,
    )
    assert plan["edges"] == []
    assert plan["capped"] == 3          # first ref trips the cap -> break


def test_plan_cap_hit_midway_binds_first_ref_only():
    # 2 parents already bound + 2 refs = 4 > 3. First ref fits, second caps.
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "node3", 2: "node2"}, attached_parents_count=2,
        bound_parent_ids={"a", "b"}, slot=2,
    )
    assert [e["ref"] for e in plan["edges"]] == [3]
    assert plan["capped"] == 2
    assert plan["attached_parents_count"] == 3


def test_plan_reports_missing_parent_node():
    # Caller raises HTTPException(500) on this — a ref whose node wasn't
    # created yet means the import walked images out of order.
    plan = _v859_plan_chain_edges(
        [3, 2], {3: "node3"}, attached_parents_count=0,
        bound_parent_ids=set(), slot=1,
    )
    assert plan["missing"] == 2


def test_plan_cap_is_checked_before_missing_lookup():
    # Ordering guard: a full slate short-circuits BEFORE the node lookup, so a
    # missing node behind a cap warns instead of raising a 500.
    plan = _v859_plan_chain_edges(
        [3], {}, attached_parents_count=3, bound_parent_ids={"a", "b", "c"}, slot=3,
    )
    assert plan["capped"] == 3
    assert plan["missing"] is None


def test_plan_does_not_mutate_caller_bound_parent_ids():
    bound = {"persona"}
    _v859_plan_chain_edges(
        [1], {1: "node1"}, attached_parents_count=1, bound_parent_ids=bound, slot=1,
    )
    assert bound == {"persona"}   # caller applies post-state explicitly


# ===========================================================================
# v859 landmine — v619 N5 must keep the scalar and the list consistent
# ===========================================================================
# N5 cleared ONLY the scalar `reference_image`. Once edge creation reads the
# LIST, an N5 drop would be silently ignored and the dropped ref would come
# back. These tests pin the invariant T1 established:
#     reference_images == []  <=>  reference_image is None
#     reference_image == reference_images[0]


def _img(index, refs):
    return {"image_index": index, "reference_image": refs[0] if refs else None,
            "reference_images": list(refs)}


def test_n5_keeps_valid_refs_untouched():
    img = _img(3, [2, 1])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == []
    assert img["reference_images"] == [2, 1]
    assert img["reference_image"] == 2


def test_n5_drops_invalid_second_ref_from_list_not_just_scalar():
    # THE LANDMINE. Pre-fix: the list kept image_9 and edge creation resurrected
    # it. reference_image (scalar, = 1) was valid so N5 didn't even fire.
    img = _img(3, [1, 9])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == [9]
    assert img["reference_images"] == [1]
    assert img["reference_image"] == 1


def test_n5_drops_invalid_first_ref_and_promotes_survivor_to_scalar():
    # Invariant: scalar is ALWAYS reference_images[0], never a stale entry.
    img = _img(3, [9, 1])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == [9]
    assert img["reference_images"] == [1]
    assert img["reference_image"] == 1


def test_n5_all_refs_invalid_clears_both_representations():
    img = _img(3, [9, 8])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == [9, 8]
    assert img["reference_images"] == []
    assert img["reference_image"] is None


def test_n5_drops_forward_ref():
    # ref >= image_index. image_4 exists but is forward of image 3.
    img = _img(3, [4])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3, 4}) == [4]
    assert img["reference_images"] == []
    assert img["reference_image"] is None


def test_n5_drops_self_ref():
    img = _img(3, [3])
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == [3]
    assert img["reference_image"] is None


def test_n5_legacy_image_without_list_key_still_clears_scalar():
    # Legacy-format images (image_platform.py ~L4592) carry ONLY the scalar —
    # no reference_images key. N5 must not crash and must not invent the key.
    img = {"image_index": 3, "reference_image": 9}
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == [9]
    assert img["reference_image"] is None
    assert "reference_images" not in img


def test_n5_legacy_valid_scalar_untouched():
    img = {"image_index": 3, "reference_image": 1}
    assert _v619_n5_drop_invalid_chain_refs(img, 3, {1, 2, 3}) == []
    assert img["reference_image"] == 1
    assert "reference_images" not in img
