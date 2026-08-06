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
from types import SimpleNamespace
from fastapi import HTTPException

from image_platform import (
    _parse_image_blocks_new,
    _parse_scene_blocks_legacy,
    _v619_n5_drop_invalid_chain_refs,
    _v859_all_parents_ready,
    _v859_collect_gating_parents,
    _v859_plan_chain_edges,
    _v859_refuse_multiref_without_ingredients,
    _max_parents,
    _normalize_external_reference_bindings,
    ExternalReferenceRef,
)


def test_reference_limit_is_model_aware():
    assert _max_parents("nano_banana_2") == 14
    assert _max_parents("nano_banana_pro") == 14
    assert _max_parents("imagen_4") == 3
    assert _max_parents("unknown-route") == 3


def test_chain_planner_accepts_more_than_three_when_model_budget_allows_it():
    refs = list(range(1, 11))
    plan = _v859_plan_chain_edges(
        refs,
        {ref: 100 + ref for ref in refs},
        attached_parents_count=0,
        bound_parent_ids=set(),
        slot=0,
        max_parents=14,
    )

    assert len(plan["edges"]) == 10
    assert plan["capped"] is None


def test_external_reference_binding_is_absent_by_default():
    req = SimpleNamespace(external_references=None, model="nano_banana_2")
    assert _normalize_external_reference_bindings(
        req,
        [{"image_index": 1}],
        db=SimpleNamespace(),
        current_user=SimpleNamespace(id="u1"),
    ) == {}


def test_external_reference_binding_accepts_open_role_and_instruction():
    parent = SimpleNamespace(
        id=22,
        user_id="u1",
        kind="upload",
        status="ready",
        chosen_variant_id=5,
    )

    class FakeQuery:
        def filter(self, *args):
            return self

        def first(self):
            return parent

    db = SimpleNamespace(query=lambda model: FakeQuery())
    ref = ExternalReferenceRef(
        parent_node_id=22,
        role="  unusual texture role  ",
        reference_instruction="  Take only the woven texture.  ",
    )
    req = SimpleNamespace(
        external_references={"image_1": [ref]},
        model="nano_banana_2",
    )

    result = _normalize_external_reference_bindings(
        req,
        [{"image_index": 1}],
        db=db,
        current_user=SimpleNamespace(id="u1"),
    )

    assert result[1][0].role == "unusual texture role"
    assert result[1][0].reference_instruction == "Take only the woven texture."


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


# ===========================================================================
# v859 — job-start gating must wait on EVERY reference
# ===========================================================================
# The bug: attached_parents was built from the SCALAR only, so parent #2 was
# never readiness-checked. can_start went True, the node went `queued`, and
# Banana 2 rendered against a reference with no chosen variant — correct edges,
# wrong render, no error. _promote_ready_children can NOT rescue it: it only
# looks at nodes still in `draft`, and this one is already `queued`.


class _FakeNode:
    """Stands in for ImageNode. can_start reads exactly these 3 attrs."""

    def __init__(self, node_id, status="ready", chosen_variant_id=7):
        self.id = node_id
        self.status = status
        self.chosen_variant_id = chosen_variant_id


def test_gating_waits_on_every_reference_not_just_the_first():
    # THE RACE. refs [3, 2]: image_3 is ready, image_2 is NOT. Pre-fix the
    # scalar (3) was the only gate -> can_start True -> queued against an
    # unready image_2. This test FAILS on the old one-parent gating.
    img = _img(4, [3, 2])
    nodes = {3: _FakeNode(3), 2: _FakeNode(2, status="generating", chosen_variant_id=None)}
    gate_refs, parents = _v859_collect_gating_parents(img, img["reference_image"], nodes)
    assert gate_refs == [3, 2]
    assert [p.id for p in parents] == [3, 2]      # BOTH are gating deps
    assert _v859_all_parents_ready(parents) is False


def test_gating_all_references_ready_starts():
    img = _img(4, [3, 2])
    nodes = {3: _FakeNode(3), 2: _FakeNode(2)}
    _, parents = _v859_collect_gating_parents(img, img["reference_image"], nodes)
    assert _v859_all_parents_ready(parents) is True


def test_gating_single_ref_unchanged_from_pre_v859():
    img = _img(3, [1])
    nodes = {1: _FakeNode(1)}
    gate_refs, parents = _v859_collect_gating_parents(img, img["reference_image"], nodes)
    assert gate_refs == [1]
    assert [p.id for p in parents] == [1]
    assert _v859_all_parents_ready(parents) is True


def test_gating_legacy_dict_without_list_key_uses_scalar():
    img = {"image_index": 3, "reference_image": 1}
    nodes = {1: _FakeNode(1)}
    gate_refs, parents = _v859_collect_gating_parents(img, 1, nodes)
    assert gate_refs == [1]
    assert [p.id for p in parents] == [1]


def test_gating_unresolvable_ref_is_skipped_not_none_appended():
    # Pre-existing `if rp is not None` guard: a ref with no created node must
    # not land in attached_parents as None (that would force can_start False
    # forever rather than raising).
    img = _img(4, [3, 2])
    gate_refs, parents = _v859_collect_gating_parents(img, 3, {3: _FakeNode(3)})
    assert gate_refs == [3, 2]
    assert [p.id for p in parents] == [3]


def test_all_parents_ready_rejects_unready_status():
    assert _v859_all_parents_ready([_FakeNode(1, status="generating")]) is False


def test_all_parents_ready_rejects_missing_chosen_variant():
    assert _v859_all_parents_ready([_FakeNode(1, chosen_variant_id=None)]) is False


def test_all_parents_ready_rejects_none_parent():
    assert _v859_all_parents_ready([None]) is False


def test_all_parents_ready_empty_is_true():
    # Matches the pre-v859 inline loop: no parents -> nothing to wait on.
    assert _v859_all_parents_ready([]) is True


# --- Safety-net equivalence: `not gate_refs` must equal `ref_image is None` ---
# The gating rewrite swaps the safety-net condition from `ref_image is None` to
# `not gate_refs`. If those ever diverge, a normal single-ref build either
# never queues or loses its subject fallback. The equivalence RESTS on T1
# (reference_images == [] <=> scalar is None), which N5 now preserves.

@pytest.mark.parametrize("refs", [[], [1], [2, 1]])
def test_gate_refs_empty_iff_scalar_is_none_new_format(refs):
    img = _img(3, refs)
    gate_refs, _ = _v859_collect_gating_parents(img, img["reference_image"], {})
    assert (not gate_refs) == (img["reference_image"] is None)


@pytest.mark.parametrize("scalar", [None, 1])
def test_gate_refs_empty_iff_scalar_is_none_legacy_format(scalar):
    img = {"image_index": 3, "reference_image": scalar}
    gate_refs, _ = _v859_collect_gating_parents(img, scalar, {})
    assert (not gate_refs) == (scalar is None)


# ===========================================================================
# v859 — multi-ref REFUSED on the legacy single-subject path
# ===========================================================================
# The no-Ingredients import path binds one `role="reference"` edge from the
# scalar. Different role vocabulary + slot semantics, so v859 is not
# generalized there — but it must not silently drop ref #2 either (the exact
# silent-partial-loss class T3 removed from the legacy scene parser).


def test_multiref_without_ingredients_raises():
    with pytest.raises(HTTPException) as exc:
        _v859_refuse_multiref_without_ingredients(_img(3, [2, 1]), 3)
    assert exc.value.status_code == 400
    assert "requires an '## Ingredients' block" in exc.value.detail
    assert "image_2, image_1" in exc.value.detail


def test_single_ref_without_ingredients_still_works():
    assert _v859_refuse_multiref_without_ingredients(_img(3, [1]), 3) is None


def test_no_ref_without_ingredients_still_works():
    assert _v859_refuse_multiref_without_ingredients(_img(3, []), 3) is None


def test_legacy_dict_without_list_key_without_ingredients_still_works():
    assert _v859_refuse_multiref_without_ingredients(
        {"image_index": 3, "reference_image": 1}, 3
    ) is None


# ===========================================================================
# v859 — DISTINCT semantic phrase per chain ref in slot translation
# ===========================================================================
# _resolve_flow_prompt_bindings rewrites the author's semantic phrases into
# Banana 2's positional slot numbers. Pre-v859 BOTH chain edges targeted the
# same phrase ("the prior-scene reference image"): the first .replace() ate
# every occurrence, the second found nothing and bound a reference the prompt
# never names — which Banana 2 blends as generic visual context, exactly the
# failure v859 exists to prevent.
#
# The phrase is keyed on chain ORDER, not slot number: persona/product occupy
# earlier slots, so slot number and chain order diverge whenever a product is
# bound (see test_product_in_mix_chain_keys_on_order_not_slot).
#
#   chain 0 -> "the prior-scene reference image"   (pose + held objects)
#              alias "the previous scene's reference image"
#   chain 1 -> "the body reference image"          (the body)
#
# The function takes only `node` and reads `node.prompt`, so fakes suffice.

from image_platform import _resolve_flow_prompt_bindings


# NB: named _Slot* rather than _Fake* — `_FakeNode` is already taken above by
# the gating stand-in, and a redefinition here would silently rebind it for
# the earlier tests too (they resolve the global at call time).
#
# `sib` is the RAW `scene_index_in_batch` column value, spelled out rather
# than derived, because the column has TWO live conventions and a fixture
# that hides which one it is under a name like `parent_idx` tests nothing in
# particular (see the KNOWN DEFECT note on _resolve_flow_prompt_bindings):
#
#   PRODUCTION (dominant)  `scene_index_in_batch=image_index`  -> 1-BASED.
#       markdown `### Image 3` stores sib=3. Use _sib_prod().
#   CODE-CONVENTION        `md = parent.scene_index_in_batch + 1` -> 0-BASED.
#       the legacy pass's own reading; markdown Image 3 would be sib=2.
#       Use _sib_code(). Only real for backfilled nodes whose name lacks
#       "Scene N".
#
# Tests state which convention they mean. The production shape is the one
# that ships, so it gets its own regression test below.


def _sib_prod(md_image_num):
    """RAW sib as the PRODUCTION import path writes it (1-based)."""
    return md_image_num


def _sib_code(md_image_num):
    """RAW sib as the legacy pass's `+1` reader assumes (0-based)."""
    return md_image_num - 1


class _SlotParent:
    def __init__(self, sib):
        self.scene_index_in_batch = sib   # RAW column value — see note above
        self.kind = "generated"


class _SlotEdge:
    def __init__(self, slot, kind, role, sib=None):
        self.slot_order, self.kind, self.role = slot, kind, role
        self.parent = _SlotParent(sib) if sib is not None else None


class _SlotNode:
    def __init__(self, edges, prompt):
        self.parent_edges, self.id, self.prompt = edges, 1, prompt


def test_two_chain_phrases_map_to_distinct_slots():
    # persona=slot0 -> Image 1, chain#0=slot1 -> Image 2, chain#1=slot2 -> Image 3
    body = ("Use the uploaded character reference image for the main character. "
            "Use the prior-scene reference image for the pose. "
            "Use the body reference image for his build.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_3", sib=_sib_code(3)),
        _SlotEdge(2, "chain", "chain_from_image_2", sib=_sib_code(2)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 1 for the main character" in out
    assert "Image 2 for the pose" in out
    assert "Image 3 for his build" in out
    assert "reference image" not in out.replace("uploaded character reference image", "")


def test_single_chain_binds_correctly():
    """One chain edge -> persona=Image 1, chain=Image 2, body phrase untouched.

    NOT named "unchanged_pre_v859": this asserts CORRECTNESS, not invariance.
    Against pre-v859 code with this same fixture the old legacy pass emitted
    "Use Image 2 for the main character" — it rewrote the PERSONA's own
    substituted "Image 1" (md=1 -> flow=2). So old code FAILS this test. The
    v859 sentinel guard is what makes it pass.
    """
    body = ("Use the uploaded character reference image for the main character. "
            "Use the prior-scene reference image for the composition.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_1", sib=_sib_code(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 1 for the main character" in out
    assert "Image 2 for the composition" in out
    # the body phrase must NOT be touched when there is only one chain edge
    assert "the body reference image" not in out


# --- THE shipped regression: SINGLE-ref, production 1-based sib ------------
# This is the shape v859's sentinel guard actually fixes in production, and
# nothing else in this file pins it. `reference_image: image_1` on Image 3
# with persona + product bound:
#     chain parent sib=1 (1-based) -> md=2, flow=3 -> 2 != 3 -> the legacy
#     `\bImage 2\b` pass ate the PRODUCT's own substituted "Image 2".
#     OLD: "Nuri from Image 1 holds the jar from Image 3, matching Image 3."
#     NEW: "Nuri from Image 1 holds the jar from Image 2, matching Image 3."
# One chain edge, one ref — the 2-ref feature is not involved. Verified
# against a70bc40^ in an isolated subprocess.

def test_production_single_ref_persona_product_chain_product_keeps_its_slot():
    body = ("Nuri from the uploaded character reference image holds the jar "
            "from the uploaded product reference image, matching "
            "the prior-scene reference image.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "product", "product"),
        _SlotEdge(2, "chain", "chain_from_image_1", sib=_sib_prod(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert out == ("Nuri from Image 1 holds the jar from Image 2, "
                   "matching Image 3.")
    # the exact pre-v859 corruption, spelled out so a regression is legible
    assert "holds the jar from Image 3" not in out


def test_production_single_ref_persona_chain_only_is_the_masked_shape():
    """persona + 1 chain, production sib: md == flow -> legacy `continue`.

    This is the shape the off-by-one accidentally masks, which is why the bug
    went unnoticed: sib=1 -> md=2, flow=2 -> equal -> the legacy pass never
    fires. Byte-identical old vs new. Pinned so that correcting the
    off-by-one later cannot quietly change it without a red test.
    """
    body = ("Nuri from the uploaded character reference image, matching "
            "the prior-scene reference image.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_1", sib=_sib_prod(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert out == "Nuri from Image 1, matching Image 2."


def test_chain_zero_alias_phrase_still_translates():
    # The pre-v859 alias must keep working for chain 0 — it is the same rung
    # of the ladder as the primary phrase, just the author's other wording.
    body = ("Use the uploaded character reference image for the main character. "
            "Use the previous scene's reference image for the composition.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_1", sib=_sib_code(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 2 for the composition" in out


def test_two_chain_edges_alias_form_for_chain_zero():
    # chain 0 written with the alias, chain 1 with the body phrase.
    body = ("Use the uploaded character reference image for the main character. "
            "Use the previous scene's reference image for the pose. "
            "Use the body reference image for his build.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_3", sib=_sib_code(3)),
        _SlotEdge(2, "chain", "chain_from_image_2", sib=_sib_code(2)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 2 for the pose" in out
    assert "Image 3 for his build" in out


def test_product_in_mix_chain_keys_on_order_not_slot():
    # persona=slot0 -> Image 1, product=slot1 -> Image 2, chain#0=slot2 -> Image 3.
    # The chain edge is at slot 2 but is chain ORDER 0, so it must claim the
    # chain-0 phrase. Keying the phrase on slot number would look for the
    # chain-1 phrase here and bind an unnamed reference.
    body = ("Use the uploaded character reference image for the main character. "
            "Use the uploaded product reference image for the jar. "
            "Use the prior-scene reference image for the pose.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "product", "product"),
        _SlotEdge(2, "chain", "chain_from_image_1", sib=_sib_code(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 1 for the main character" in out
    assert "Image 2 for the jar" in out
    assert "Image 3 for the pose" in out


def test_product_plus_two_chains_all_four_slots():
    # persona=1, product=2, chain#0=3, chain#1=4 — the full v859 house.
    body = ("Use the uploaded character reference image for the main character. "
            "Use the uploaded product reference image for the jar. "
            "Use the prior-scene reference image for the pose. "
            "Use the body reference image for his build.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "product", "product"),
        _SlotEdge(2, "chain", "chain_from_image_4", sib=_sib_code(4)),
        _SlotEdge(3, "chain", "chain_from_image_2", sib=_sib_code(2)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 1 for the main character" in out
    assert "Image 2 for the jar" in out
    assert "Image 3 for the pose" in out
    assert "Image 4 for his build" in out


def test_body_phrase_untranslated_when_only_one_chain_edge():
    # A body that names the chain-1 phrase but declares only ONE ref. The
    # phrase stays LITERAL: there is no second image to bind it to, so
    # translating it would point at a slot that does not exist. Leaving it
    # readable ("the body reference image") degrades to a plain English
    # instruction Banana 2 can still act on against the images it has —
    # the same robustness argument v589.1 makes for the semantic phrases.
    body = ("Use the uploaded character reference image for the main character. "
            "Use the prior-scene reference image for the pose. "
            "Use the body reference image for his build.")
    node = _SlotNode([
        _SlotEdge(0, "character", "persona"),
        _SlotEdge(1, "chain", "chain_from_image_1", sib=_sib_code(1)),
    ], body)
    out = _resolve_flow_prompt_bindings(node)
    assert "Image 2 for the pose" in out
    assert "the body reference image for his build" in out
