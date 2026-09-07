"""Small, dependency-free contract shared by image QC and the server.

The model is useful for describing what it saw.  It is not trusted with
identity, freshness, or the final choice.  This module turns the current
node, its references, and the candidate bytes into a stable snapshot and
checks the two complete reads that are required before an automatic choice.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

AUTO_CONTRACT_VERSION = 1
REPORT_VERSION = 1
RUBRIC_VERSION = "image-reference-qc-v1"
MAX_REFERENCES = 8
MAX_FINDINGS = 8
MAX_STRING_CHARS = 240
MAX_SNAPSHOT_BYTES = 24_000

CHILD_FIELDS = (
    "id", "kind", "origin", "status", "chosen_variant_id", "prompt",
    "aspect_ratio", "resolution", "model", "n_variants", "action_note",
    "frame_anchor_s", "visual_delta", "narrative_lens", "cast", "role",
    "pair_role", "paired_with_image_node_id", "batch_id",
    "scene_index_in_batch",
)

HARD_FAIL_CODES = frozenset({
    "compliance", "text_error", "identity_error",
    "required_element_missing", "corruption",
})
REFERENCE_FAIL_CODES = {
    "identity": frozenset({"different_person", "face_not_observable"}),
    "product": frozenset({
        "product_missing", "brand_text_mismatch", "dosage_text_mismatch",
        "gross_container_mismatch", "hero_text_unreadable",
    }),
    "continuity": frozenset({
        "required_change_missing", "unexpected_identity",
        "unexpected_wardrobe", "unexpected_setting", "unexpected_product",
        "unexpected_object_state", "lighting_change_warning",
    }),
    "body": frozenset({"instruction_mismatch", "reference_not_observable"}),
    "support": frozenset({"instruction_mismatch", "reference_not_observable"}),
    "role": frozenset({"instruction_mismatch", "reference_not_observable"}),
}
AUTO_INTENTS = frozenset({"identity", "product"})
MANUAL_INTENTS = frozenset({"continuity", "body", "support", "role"})
_MULTI_PERSON_RE = re.compile(
    r"(?:\b(?:two|three|four|several|multiple)\s+"
    r"(?:adults?|people|persons?|men|women|characters?|figures?)\b|"
    r"\b(?:couple|duo|group|crowd|husband\s+and\s+wife|"
    r"wife\s+and\s+husband)\b|"
    r"\b(?:another|second|additional)\s+(?:person|adult|man|woman|"
    r"character|figure)\b|"
    r"\b(?:(?:a|one|the)\s+)?(?:woman|man|wife|husband)\s+"
    r"(?:and|with|beside|next\s+to|alongside)\s+"
    r"(?:(?:her|his|a|the)\s+)?(?:husband|wife|woman|man)\b|"
    r"\b(?:woman|man|wife|husband)\s+and\s+"
    r"(?:woman|man|wife|husband)\b|"
    r"\b(?:a|one)\s+(?:woman|man)\s+"
    r"(?:with|beside|next\s+to|alongside)\s+"
    r"(?:(?:her|his)\s+)?(?:husband|wife|woman|man)\b)",
    re.IGNORECASE,
)


def _int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _bounded(value: Any) -> str:
    return _text(value)[:MAX_STRING_CHARS]


def _bounded_list(value: Any) -> List[str]:
    if value is None:
        return []
    values = value if isinstance(value, (list, tuple)) else [value]
    return [_bounded(v) for v in values[:MAX_FINDINGS] if _bounded(v)]


def _valid_bounded_strings(value: Any) -> bool:
    return (isinstance(value, list) and len(value) <= MAX_FINDINGS and
            all(isinstance(item, str) and len(item) <= MAX_STRING_CHARS
                for item in value))


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def is_multi_person_prompt(prompt: Any) -> bool:
    """Conservative shared check for prompts that name more than one person."""
    return bool(_MULTI_PERSON_RE.search(_text(prompt)))


def canonical_reference_class(edge: Any) -> str:
    """Use one class vocabulary for rendering, wire payloads, and QC."""
    kind = _text(_get(edge, "kind")).lower()
    role = _text(_get(edge, "role")).lower()
    if kind in {"character", "persona", "subject"}:
        return "character"
    if kind == "product":
        return "product"
    if role.startswith("variant_chain:"):
        return "character"
    if role.startswith("chain_from_image_"):
        return "chain"
    if role in {"subject", "persona", "character"}:
        return "character"
    if role == "product":
        return "product"
    if role in {"reference", "chain"}:
        return "chain"
    parent = _get(edge, "parent")
    if _text(_get(parent, "kind")).lower() == "generated":
        return "chain"
    return "other"


def canonical_reference_intent(edge: Any, chain_sequence: int = 0) -> str:
    declared = _text(_get(edge, "reference_intent")).lower()
    cls = canonical_reference_class(edge)
    # Serialized QC payloads use only explicit edge meaning.  Renderer-only
    # positional chain jobs are intentionally not allowed to leak here:
    # chain_from_image_* proves continuity; a generic chain is role.
    if cls == "chain":
        role = _text(_get(edge, "role")).lower()
        return "continuity" if role.startswith("chain_from_image_") else "role"
    if declared in {"identity", "product", "continuity", "body", "support", "role"}:
        return declared
    if cls == "character":
        return "identity"
    if cls == "product":
        return "product"
    return "role"


def _cast(value: Any) -> Any:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return value.strip()
    if isinstance(value, (list, tuple)):
        return [_text(x) for x in value]
    return value


def _sha(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        # A caller may already have a digest; only accept the exact form.
        if len(value) == 64 and all(c in "0123456789abcdef" for c in value.lower()):
            return value.lower()
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return hashlib.sha256(bytes(value)).hexdigest()
    return None


def _bytes_for(mapping: Any, ident: Any) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    return mapping.get(ident, mapping.get(str(ident)))


def _child_snapshot(node: Any) -> Dict[str, Any]:
    return {field: (_cast(_get(node, field)) if field == "cast"
                    else _get(node, field)) for field in CHILD_FIELDS}


def build_input_snapshot(
    child: Any,
    candidates: Optional[Sequence[Any]] = None,
    edges: Optional[Sequence[Any]] = None,
    parents: Optional[Sequence[Any]] = None,
    candidate_bytes: Optional[Mapping[Any, Any]] = None,
    parent_bytes: Optional[Mapping[Any, Any]] = None,
    rubric_version: str = RUBRIC_VERSION,
) -> Dict[str, Any]:
    """Build the complete current input snapshot.

    Missing bytes remain ``None`` so a missing file can never look fresh.  The
    result is JSON data only; both the server and the local scorer use it.
    """
    candidates = list(candidates if candidates is not None
                      else (_get(child, "variants", []) or []))
    edges = list(edges if edges is not None
                 else (_get(child, "parents", []) or []))
    parents = list(parents or [])
    parent_by_id = {_int(_get(p, "id")): p for p in parents
                    if _int(_get(p, "id")) is not None}
    edge_rows: List[Dict[str, Any]] = []
    chain_sequence = 0
    for index, edge in enumerate(edges[:MAX_REFERENCES]):
        cls = canonical_reference_class(edge)
        intent = canonical_reference_intent(edge, chain_sequence)
        if cls == "chain" and not _text(_get(edge, "reference_intent")):
            chain_sequence += 1
        edge_rows.append({
            "id": _int(_get(edge, "id")),
            "slot": _int(_get(edge, "slot_order", index)) or 0,
            "class": cls,
            "intent": intent,
            "role": _text(_get(edge, "role")),
            "instruction": _text(_get(edge, "reference_instruction")),
            "origin": _text(_get(edge, "origin")) or "manual",
            "parent_node_id": _int(_get(edge, "parent_node_id")),
        })
    edge_rows.sort(key=lambda row: (row["slot"], row["id"] or -1))

    candidate_rows = []
    for candidate in candidates:
        ident = _int(_get(candidate, "id"))
        candidate_rows.append({"id": ident,
                               "sha256": _sha(_bytes_for(candidate_bytes, ident))})
    candidate_rows.sort(key=lambda row: row["id"] if row["id"] is not None else -1)

    parent_rows = []
    for parent_id, parent in parent_by_id.items():
        chosen_id = _int(_get(parent, "chosen_variant_id"))
        parent_rows.append({
            "id": parent_id,
            "status": _text(_get(parent, "status")),
            "chosen_variant_id": chosen_id,
            "origin": _text(_get(parent, "origin")) or "manual",
            "chosen_image_sha256": _sha(_bytes_for(parent_bytes, chosen_id)),
        })
    parent_rows.sort(key=lambda row: row["id"] if row["id"] is not None else -1)
    return {
        "rubric_version": rubric_version,
        "child": _child_snapshot(child),
        "candidates": candidate_rows,
        "edges": edge_rows,
        "parents": parent_rows,
    }


def snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    data = json.dumps(snapshot, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def snapshot_size(snapshot: Mapping[str, Any]) -> int:
    """Return the UTF-8 size used by the strict snapshot budget."""
    try:
        return len(json.dumps(snapshot, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"), allow_nan=False).encode("utf-8"))
    except (TypeError, ValueError):
        return MAX_SNAPSHOT_BYTES + 1


def build_snapshot_and_digest(*args: Any, **kwargs: Any) -> Tuple[Dict[str, Any], str]:
    snapshot = build_input_snapshot(*args, **kwargs)
    return snapshot, snapshot_digest(snapshot)


def _valid_id_list(values: Any, expected: Sequence[int]) -> bool:
    if not isinstance(values, list):
        return False
    got = [_int(value) for value in values]
    return all(v is not None for v in got) and len(got) == len(set(got)) \
        and set(got) == set(expected)


def _edge_intents(node: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    edges = node.get("parents")
    if not isinstance(edges, list):
        return [], ["missing reference edges"]
    rows = []
    for edge in edges[:MAX_REFERENCES + 1]:
        if not isinstance(edge, Mapping):
            return [], ["malformed reference edge"]
        row = dict(edge)
        row["id"] = _int(row.get("id"))
        row["intent"] = row.get("reference_intent") or canonical_reference_intent(edge)
        row["class"] = row.get("reference_class") or canonical_reference_class(edge)
        rows.append(row)
    return rows, []


def _candidate_passes(row: Mapping[str, Any], expected_ref_ids: Sequence[int]) -> bool:
    if _int(row.get("variant_id")) is None:
        return False
    if row.get("prompt_status") != "pass":
        return False
    if row.get("required_elements_visible") is not True:
        return False
    if _bounded_list(row.get("element_misses")):
        return False
    hard = row.get("hard_fail_codes")
    if not isinstance(hard, list) or hard:
        return False
    refs = row.get("references")
    return isinstance(refs, list) and len(refs) == len(expected_ref_ids)


def validate_auto_decision(
    node: Mapping[str, Any],
    report: Optional[Mapping[str, Any]],
    current_snapshot: Optional[Mapping[str, Any]] = None,
    current_digest: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a closed ``choose``/``review`` result.  Never raises."""
    def review(reason: str, survivors: Optional[List[int]] = None) -> Dict[str, Any]:
        vals = list(survivors or [])
        return {"decision": "review", "action": "review", "reason": reason,
                "reasons": [reason], "variant_id": None,
                "chosen_variant_id": None, "survivors": vals,
                "eligible_survivors": vals}

    if not isinstance(node, Mapping) or node.get("kind") != "generated":
        return review("node is not a generated image")
    if node.get("chosen_variant_id") is not None:
        return review("node already has a chosen variant")
    if _text(node.get("origin", "manual")).lower() in {"auto", "automatic", "scraped"}:
        return review("automatic-origin node")
    prompt = _text(node.get("prompt")).lower()
    if (node.get("multi_person") is True or node.get("multi_character") is True or
            is_multi_person_prompt(prompt)):
        return review("multi-person requirement")
    variants = node.get("variants")
    if not isinstance(variants, list) or not variants:
        return review("no usable variants")
    ids = [_int(_get(v, "id")) for v in variants if isinstance(v, Mapping)]
    if len(ids) != len(variants) or len(set(ids)) != len(ids):
        return review("malformed or duplicate candidate ids")
    if any(_text(_get(v, "source") or "ai").lower() != "ai"
           for v in variants if isinstance(v, Mapping)):
        return review("manual candidate variant")
    edges, errors = _edge_intents(node)
    if errors:
        return review(errors[0])
    if len(edges) == 0:
        return review("no-reference node")
    if len(edges) > MAX_REFERENCES:
        return review("too many references")
    if any(e.get("intent") in MANUAL_INTENTS for e in edges):
        return review("reference intent remains manual")
    if any(e.get("intent") not in AUTO_INTENTS for e in edges):
        return review("unsupported reference intent")
    for edge in edges:
        if (_text(edge.get("parent_status")).lower() != "ready" or
                _int(edge.get("parent_chosen_variant_id")) is None):
            return review("reference parent is not ready and chosen")
    if any(e.get("intent") in {"body", "support", "role"} and
           not _text(e.get("reference_instruction")) for e in edges):
        return review("reference instruction required")
    if sum(e.get("intent") == "identity" for e in edges) > 1:
        return review("multiple character references")
    if sum(e.get("intent") == "product" for e in edges) > 1:
        return review("multiple product references")
    if any(_text(e.get("origin")).lower() in {"auto", "automatic", "scraped"} or
           _text(e.get("parent_origin")).lower() in {"auto", "automatic", "scraped"}
           for e in edges):
        return review("automatic-origin reference")
    if not isinstance(report, Mapping) or report.get("version") != REPORT_VERSION:
        return review("missing or malformed QC report")
    if report.get("auto_contract_version") != AUTO_CONTRACT_VERSION:
        return review("missing auto contract version")
    if report.get("rubric_version") != RUBRIC_VERSION:
        return review("QC rubric is stale")
    digest = report.get("snapshot_digest")
    if (not isinstance(digest, str) or len(digest) != 64 or
            any(c not in "0123456789abcdef" for c in digest.lower())):
        return review("missing snapshot digest")
    if not isinstance(report.get("input_snapshot"), Mapping):
        return review("missing input snapshot")
    if snapshot_size(report["input_snapshot"]) > MAX_SNAPSHOT_BYTES:
        return review("QC input snapshot is over the size limit")
    if snapshot_digest(report["input_snapshot"]) != digest:
        return review("QC report snapshot digest does not bind to its snapshot")
    if current_snapshot is not None:
        if snapshot_size(current_snapshot) > MAX_SNAPSHOT_BYTES:
            return review("current QC input snapshot is over the size limit")
        expected = snapshot_digest(current_snapshot)
        if digest != expected:
            return review("QC report snapshot is stale")
    elif current_digest is not None and digest != current_digest:
        return review("QC report snapshot is stale")
    reads = report.get("reads")
    if not isinstance(reads, list) or len(reads) != 2:
        return review("QC report does not contain two complete reads")
    expected_edges = [_int(e.get("id")) for e in edges]
    if any(e is None for e in expected_edges) or len(set(expected_edges)) != len(expected_edges):
        return review("reference edge ids are malformed")
    read_maps: List[Dict[int, Mapping[str, Any]]] = []
    identity_edge_ids = [_int(e.get("id")) for e in edges
                         if e.get("intent") == "identity"]
    face_scores = report.get("reference_face_sim")
    variant_reports = report.get("variants")
    if not isinstance(variant_reports, Mapping):
        return review("missing per-variant integrity evidence")
    try:
        stored_ids = {_int(key) for key in variant_reports}
    except Exception:
        stored_ids = set()
    if None in stored_ids or stored_ids != set(ids):
        return review("per-variant integrity set is stale")
    for expected_read, read in enumerate(reads, 1):
        if (not isinstance(read, Mapping) or read.get("read_id") != expected_read
                or read.get("snapshot_digest") != digest):
            return review("read ids are missing or out of order")
        rows = read.get("candidates")
        if not isinstance(rows, list) or len(rows) != len(ids):
            return review("candidate read is incomplete")
        cmap: Dict[int, Mapping[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                return review("malformed candidate read")
            vid = _int(row.get("variant_id"))
            if vid is None or vid in cmap or vid not in ids:
                return review("candidate ids are missing, duplicate, or extra")
            if row.get("person_count") not in {"zero", "one", "multiple", "unknown"}:
                return review("invalid candidate person count")
            refs = row.get("references")
            if not isinstance(refs, list) or len(refs) != len(expected_edges):
                return review("reference read is incomplete")
            seen_edges = set()
            for ref in refs:
                if not isinstance(ref, Mapping):
                    return review("malformed reference read")
                eid = _int(ref.get("edge_id"))
                if eid is None or eid in seen_edges or eid not in expected_edges:
                    return review("reference edge ids are missing, duplicate, or extra")
                seen_edges.add(eid)
                status = ref.get("status")
                if status not in {"pass", "fail", "unknown"}:
                    return review("invalid reference status")
                if not isinstance(ref.get("observable"), bool):
                    return review("invalid reference observability")
                edge = edges[expected_edges.index(eid)]
                allowed = REFERENCE_FAIL_CODES.get(edge.get("intent"), frozenset())
                codes = ref.get("fail_codes")
                if not isinstance(codes, list) or len(codes) > MAX_FINDINGS:
                    return review("unbounded reference findings")
                if any(code not in allowed for code in codes):
                    return review("unknown reference failure code")
                observed_hero_text = ref.get("observed_hero_text")
                if (not isinstance(observed_hero_text, str) or
                        len(observed_hero_text) > MAX_STRING_CHARS):
                    return review("unbounded observed hero text")
                expected_parent = _int(edge.get("parent_chosen_variant_id"))
                if expected_parent is not None and _int(ref.get("parent_variant_id")) != expected_parent:
                    return review("reference parent choice is stale")
                if (edge.get("intent") == "product" and
                        not observed_hero_text.strip()):
                    return review("product evidence is blank")
            if len(seen_edges) != len(expected_edges):
                return review("reference edges are incomplete")
            hard = row.get("hard_fail_codes")
            if not isinstance(hard, list) or len(hard) > MAX_FINDINGS or any(c not in HARD_FAIL_CODES for c in hard):
                return review("unknown candidate failure code")
            if not _valid_bounded_strings(row.get("element_misses")):
                return review("unbounded candidate findings")
            if not _valid_bounded_strings(row.get("warnings")):
                return review("unbounded candidate warnings")
            for field in ("element_misses", "warnings"):
                if any(len(_bounded(v)) != len(_text(v)) for v in row[field]):
                    return review("unbounded candidate finding")
            if row.get("prompt_status") not in {"pass", "fail", "unknown"}:
                return review("invalid prompt status")
            if row.get("required_elements_visible") not in {True, False, "unknown"}:
                return review("invalid required-elements status")
            cmap[vid] = row
        if set(cmap) != set(ids):
            return review("candidate ids are incomplete")
        read_maps.append(cmap)
    survivors = []
    for vid in ids:
        ok = True
        counts = [cmap[vid].get("person_count") for cmap in read_maps]
        if (len(set(counts)) != 1 or counts[0] in {"multiple", "unknown"} or
                (identity_edge_ids and counts[0] != "one")):
            ok = False
        stored = variant_reports.get(str(vid), variant_reports.get(vid))
        integrity = stored.get("integrity") if isinstance(stored, Mapping) else None
        if not isinstance(integrity, Mapping) or integrity.get("ok") is not True:
            ok = False
        if identity_edge_ids:
            score = (face_scores.get(str(vid), face_scores.get(vid))
                     if isinstance(face_scores, Mapping) else None)
            if (isinstance(score, bool) or not isinstance(score, (int, float))
                    or score < 0.35):
                ok = False
        for cmap in read_maps:
            if not ok:
                break
            row = cmap[vid]
            if not _candidate_passes(row, expected_edges):
                ok = False
                break
            for ref in row["references"]:
                if ref.get("status") != "pass" or ref.get("observable") is not True or ref.get("fail_codes"):
                    ok = False
                    break
        if ok:
            survivors.append(vid)
    if len(survivors) != 1:
        return review("zero eligible QC survivors" if not survivors else
                      "multiple eligible QC survivors", survivors)
    return {"decision": "choose", "action": "choose",
            "reason": "exactly one reference-aware QC survivor",
            "reasons": [], "variant_id": survivors[0],
            "chosen_variant_id": survivors[0], "survivors": survivors,
            "eligible_survivors": survivors}


def validate_report(node: Any, report: Any = None, **kwargs: Any) -> Dict[str, Any]:
    """Compatibility alias used by callers that name the operation plainly."""
    if report is None and "report" in kwargs:
        report = kwargs.pop("report")
    return validate_auto_decision(node, report, **kwargs)


# Short names keep tests and small integrations readable without duplicating
# the canonical implementation.
make_snapshot = build_input_snapshot
compute_snapshot_digest = snapshot_digest
validate_decision = validate_auto_decision
