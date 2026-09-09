"""v965 — what a clip IS, in ONE typed place. The brain declares it; the arms obey it.

THE PROBLEM THIS SOLVES
-----------------------
The Flow worker used to decide things that belong to the video: whether a clip
runs on the Frames or the Ingredients tab, whether it needs its own project,
which reference is a face and which is a scene, and what to try when a
generation is policy-blocked. Every one of those is a guess, and a wrong guess
costs an operator a cycle. Worse, a guess is invisible: nothing in a log says
"I decided this".

So the platform decides, once, at job creation, and hands the worker a finished
object. The worker's whole job becomes: apply exactly this, prove it applied,
or refuse the clip. It decides only HOW FAST -- concurrency, retries, which
lane picks the job up.

WHY A SINGLE OBJECT AND NOT FOUR NEW COLUMNS
--------------------------------------------
Adding one field the old way costs ~13 hand-edits across two SQL dialects, two
Pydantic models, two DB tables and two near-identical polling endpoints. Four
fields would be ~50, and every one is a chance to wire it up everywhere except
the last hop -- which has already happened twice. `resolution` was declared and
dropped for months (fixed by v963.20), and `input_mode` is sent on every
movie-section clip today and never read (`main.py:19353`). One object means one
question -- "did the contract arrive?" -- with one answer.

KEEP THIS MODULE FREE OF PROJECT IMPORTS
----------------------------------------
Same discipline as `veo_models.py`, and for the same reason: the parser, the
API and the linters all need it, so it must not pull any of them in. The one
exception is `veo_models` itself, which is also a leaf and holds the model
strings this module would otherwise have to copy.

`static/flow_worker.py` does NOT import this file. It cannot -- it runs
standalone on worker machines and imports same-directory modules only
(`static/flow_worker.py:7338-7346`). That is deliberate and it shapes the whole
design: the worker walks the keys of the contract DICT it was handed, so its
idea of "every field" can never drift from what the server actually declared.
The server validates; the worker applies and proves.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from veo_models import ALLOWED_VEO_MODELS

# The contract version stamped on a clip. Bump ONLY with a migration plan:
# the worker refuses a contract whose version it does not know, so a bump that
# ships to the platform before the workers is an outage, not a no-op.
CONTRACT_VERSION = 1

# How much of a charswap source video the worker uses. A platform constant, not
# an authored value -- v943 measured the swap route against it. It lives at the
# TOP level of the contract, not inside an asset, because `main.py:19155`
# already sends it that way and because it describes the swap OPERATION (how
# much of the source is used), not one file.
SWAP_MAX_SOURCE_S = 10

# Flow's own duration tabs. 10s is real in Flow; the Veo API has no 10s bucket
# and folds 10 -> 8 (v861).
ALLOWED_DURATIONS = (4, 6, 8, 10)

ALLOWED_INPUT_MODES = ("frames", "ingredients")
ALLOWED_SWAP_MODES = ("video-led", "image-led")

# The last rung of every fallback ladder. A ladder that cannot end is a ladder
# that retries for ever, which is how a policy block once became an infinite
# swap loop (v959).
TERMINAL_RUNG = "fail"
ALLOWED_RUNGS = ("prompt_b", "model_swap", TERMINAL_RUNG)

# The three render lanes. `simple` is the ordinary path and carries no
# `render_method` bullet at all -- the column is NULL for it.
LANE_SIMPLE = "simple"
LANE_CHARSWAP = "charswap"
LANE_MOVIE_SECTION = "movie-section"
ALLOWED_LANES = (LANE_SIMPLE, LANE_CHARSWAP, LANE_MOVIE_SECTION)


# ---------------------------------------------------------------------------
# The rules, as plain functions. BOTH models call these, so "legal" has exactly
# ONE spelling. Two copies of a validator is how a parser and an API drift into
# disagreeing about the same value -- and pydantic v2 binds a @field_validator
# to the class that declares it, so sharing by copying the attribute silently
# registers nothing at all. (Tried it; it accepted every bad value.)
# ---------------------------------------------------------------------------


def check_version(v: int) -> int:
    if v != CONTRACT_VERSION:
        raise ValueError(
            f"[v965] clip_contract_version {v} is not {CONTRACT_VERSION}; "
            f"this build of the platform only writes v{CONTRACT_VERSION}")
    return v


def check_input_mode(v: str) -> str:
    if v not in ALLOWED_INPUT_MODES:
        raise ValueError(
            f"[v965] input_mode {v!r} is not one of "
            f"{' | '.join(ALLOWED_INPUT_MODES)}")
    return v


def check_ladder(v):
    """A policy ladder must be able to end. Every rung legal, `fail` last, and
    `fail` nowhere earlier -- otherwise a blocked clip either retries for ever
    or carries rungs nothing can reach."""
    if not v:
        raise ValueError(
            f"[v965] policy_fallback is empty; it must end in "
            f"{TERMINAL_RUNG!r} even when there is nothing to try first")
    unknown = [r for r in v if r not in ALLOWED_RUNGS]
    if unknown:
        raise ValueError(
            f"[v965] policy_fallback has unknown rung(s) {unknown}; "
            f"legal rungs are {', '.join(ALLOWED_RUNGS)}")
    if v[-1] != TERMINAL_RUNG:
        raise ValueError(
            f"[v965] policy_fallback must end in {TERMINAL_RUNG!r}; "
            f"{v} does not, so a blocked clip would retry for ever")
    if TERMINAL_RUNG in v[:-1]:
        raise ValueError(
            f"[v965] policy_fallback has {TERMINAL_RUNG!r} before the end; "
            f"nothing after it can run")
    return v


class Role(str, Enum):
    """What an asset is FOR. Closed on purpose.

    The worker attaches a file by its role, never by its position in a list or
    by guessing from its name. `movie_section_fetch_inputs` used to infer
    "scene vs face" from whether a url appeared in `face_ref_urls`; that
    inference is what this enum replaces.
    """

    START_FRAME = "start_frame"
    END_FRAME = "end_frame"
    FACE = "face"
    AVATAR = "avatar"
    SWAP_SOURCE = "swap_source"
    # Reserved. No producer in v1 -- see `validate_lane`, which hard-fails it.
    # It exists so a sixth kind of chip does not force an enum change plus a
    # migration, and it fails loudly rather than rotting quietly.
    REFERENCE = "reference"


class Media(str, Enum):
    """Image or video. This is the thing an image-only ref list could not say,
    and it is why charswap could not be expressed by the first sketch: its
    source is an mp4."""

    IMAGE = "image"
    VIDEO = "video"


# role -> the media it must be. A face is never a video; a swap source always
# is. Checked in AssetEntry so a mismatched pair cannot be built at all.
ROLE_MEDIA = {
    Role.START_FRAME: Media.IMAGE,
    Role.END_FRAME: Media.IMAGE,
    Role.FACE: Media.IMAGE,
    Role.AVATAR: Media.IMAGE,
    Role.SWAP_SOURCE: Media.VIDEO,
    Role.REFERENCE: Media.IMAGE,
}

# R = required, O = optional, F = forbidden. (kind, min, max).
#
# NONE OF THIS IS A NEW RULE. Every cell is a refusal the parser already makes,
# re-expressed as a table so one function can check it instead of nine scattered
# `if` blocks. The single exception is `start_frame` on a `video-led` charswap
# clip, which is newly FORBIDDEN: today a start frame is sent on every clip and
# the video-led path ignores it, and an asset nobody attaches is an asset the
# worker has to make a judgement about.
_R = "R"
_O = "O"
_F = "F"

LANE_RULES = {
    LANE_SIMPLE: {
        Role.START_FRAME: (_R, 1, 1),
        Role.END_FRAME: (_O, 0, 1),
        Role.FACE: (_F, 0, 0),
        Role.AVATAR: (_F, 0, 0),
        Role.SWAP_SOURCE: (_F, 0, 0),
        Role.REFERENCE: (_F, 0, 0),
    },
    LANE_CHARSWAP: {
        # start_frame is conditional on swap_mode and is handled in
        # `validate_lane`: required for image-led, forbidden for video-led.
        Role.START_FRAME: (None, 0, 1),
        Role.END_FRAME: (_F, 0, 0),
        Role.FACE: (_F, 0, 0),
        Role.AVATAR: (_R, 1, 1),
        Role.SWAP_SOURCE: (_R, 1, 1),
        Role.REFERENCE: (_F, 0, 0),
    },
    LANE_MOVIE_SECTION: {
        Role.START_FRAME: (_R, 1, 1),
        Role.END_FRAME: (_F, 0, 0),
        # MOVIE_SECTION_MAX_FACE_REFS = 2 (`image_platform.py:780`).
        Role.FACE: (_R, 1, 2),
        Role.AVATAR: (_F, 0, 0),
        Role.SWAP_SOURCE: (_F, 0, 0),
        Role.REFERENCE: (_F, 0, 0),
    },
}


class AssetEntry(BaseModel):
    """One file the worker attaches, and what it is for.

    Exactly five keys. `extra="forbid"` so a typo, or a key that belongs at the
    top level, is a loud error at import instead of a value silently ignored at
    render time.
    """

    model_config = ConfigDict(extra="forbid")

    role: Role
    media: Media
    # THE AUTHORITATIVE IDENTIFIER. Several incompatible id forms are in use
    # across the layers for what is conceptually the same reference (markdown
    # `image_N`, ImageNode ids, R2 keys, upload ids, proxy urls). The contract
    # picks one: the R2 key. Everything else resolves to it before it gets here.
    key: str
    # How to fetch the key in THIS lane. Derived from `key`, never identity --
    # the two worker lanes carry different credentials and so get different
    # urls for identical bytes.
    url: str
    # The markdown token this came from (`image_5`, or a declared asset name).
    # Never used for routing. It is here so a log line and an error message can
    # name what the AUTHOR wrote, which is what makes a failure readable.
    origin: str

    @model_validator(mode="after")
    def _media_matches_role(self) -> "AssetEntry":
        expected = ROLE_MEDIA[self.role]
        if self.media != expected:
            raise ValueError(
                f"[v965] asset {self.origin!r}: role {self.role.value!r} is "
                f"always {expected.value!r}, not {self.media.value!r}"
            )
        return self


class ClipContract(BaseModel):
    """Everything about a clip that the worker must not decide for itself.

    Twelve fields, and EVERY ONE IS ALWAYS PRESENT. A field that does not apply
    to this clip carries an explicit `null`. There are no optional WHAT fields,
    because an absent key and a deliberate `null` look identical to a reader and
    only one of them means "the author thought about this".

    `render_method` is deliberately NOT in here. It is the column beside this
    object, because the poll query filters on it in SQL and a JSON key cannot
    answer that portably.
    """

    model_config = ConfigDict(extra="forbid")

    clip_contract_version: int
    input_mode: str
    isolate_project: bool
    policy_fallback: List[str]
    veo_model: str
    duration_s: int
    aspect_ratio: str
    variants: int
    resolution: str
    swap_mode: Optional[str]
    swap_max_source_s: Optional[int]
    assets: List[AssetEntry]

    @field_validator("clip_contract_version")
    @classmethod
    def _known_version(cls, v: int) -> int:
        return check_version(v)

    @field_validator("input_mode")
    @classmethod
    def _known_input_mode(cls, v: str) -> str:
        return check_input_mode(v)

    @field_validator("veo_model")
    @classmethod
    def _known_model(cls, v: str) -> str:
        if v not in ALLOWED_VEO_MODELS:
            raise ValueError(
                f"[v965] veo_model {v!r} is not a label the Flow dropdown "
                f"offers; see veo_models.ALLOWED_VEO_MODELS"
            )
        return v

    @field_validator("duration_s")
    @classmethod
    def _known_duration(cls, v: int) -> int:
        if v not in ALLOWED_DURATIONS:
            raise ValueError(
                f"[v965] duration_s {v} is not one of "
                f"{'|'.join(str(d) for d in ALLOWED_DURATIONS)}"
            )
        return v

    @field_validator("variants")
    @classmethod
    def _sane_variants(cls, v: int) -> int:
        if not 1 <= v <= 4:
            raise ValueError(f"[v965] variants {v} is outside Flow's x1-x4")
        return v

    @field_validator("swap_mode")
    @classmethod
    def _known_swap_mode(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ALLOWED_SWAP_MODES:
            raise ValueError(
                f"[v965] swap_mode {v!r} is not one of "
                f"{' | '.join(ALLOWED_SWAP_MODES)} or null"
            )
        return v

    @field_validator("policy_fallback")
    @classmethod
    def _ladder_terminates(cls, v: List[str]) -> List[str]:
        return check_ladder(v)


class ClipContractDeclaration(BaseModel):
    """What the AUTHOR declared, before anything is resolved.

    WHY THIS EXISTS AND IS NOT JUST `ClipContract`
    ----------------------------------------------
    A full `ClipContract` cannot be built at parse time, because `AssetEntry`
    needs the R2 key -- and 2.3 makes the key the authoritative identity on
    purpose. The key does not exist yet. The resolution chain is markdown
    `image_N` -> `ImageNode` id at import -> R2 key at job creation
    (`_v959_materialise_face_frames`, called from `promote_batch_to_video`,
    `image_platform.py:13633`). The parser sees markdown text and nothing else.

    So the object is assembled in two stages, which is the same split the
    face-ref columns already use: `face_ref_node_ids_json` on the assignment
    row holds the authoring form, `face_ref_frames_json` on the clip holds the
    resolved form. Here:

      * this model -> `image_scene_assignments.clip_contract_json`, at import
      * `ClipContract` -> `clips.clip_contract_json`, at job creation

    Asset ORIGINS are not repeated here. They already have homes on the same
    row (`image_node_id`, `end_frame_image_node_id`, `face_ref_node_ids_json`,
    the swap columns), and copying them would create a second place for the
    same fact to drift.
    """

    model_config = ConfigDict(extra="forbid")

    clip_contract_version: int
    input_mode: str
    isolate_project: bool
    policy_fallback: List[str]

    # The SAME functions ClipContract validates with, so a declaration cannot
    # say something the finished contract would refuse.
    @field_validator("clip_contract_version")
    @classmethod
    def _known_version(cls, v: int) -> int:
        return check_version(v)

    @field_validator("input_mode")
    @classmethod
    def _known_input_mode(cls, v: str) -> str:
        return check_input_mode(v)

    @field_validator("policy_fallback")
    @classmethod
    def _ladder_terminates(cls, v: List[str]) -> List[str]:
        return check_ladder(v)


def validate_lane(contract: ClipContract, render_method: Optional[str]) -> None:
    """Check the asset list against the lane's own rules. Raises ValueError.

    `render_method` is the column, not a contract field: NULL/empty means the
    ordinary `simple` lane.
    """
    lane = (render_method or "").strip().lower() or LANE_SIMPLE
    if lane not in ALLOWED_LANES:
        raise ValueError(
            f"[v965] render_method {render_method!r} is not one of "
            f"{', '.join(ALLOWED_LANES)}"
        )

    counts = {role: 0 for role in Role}
    for entry in contract.assets:
        counts[entry.role] += 1

    if counts[Role.REFERENCE]:
        raise ValueError(
            "[v965] `reference` has no producer in contract v1"
        )

    rules = LANE_RULES[lane]
    for role, (kind, lo, hi) in rules.items():
        n = counts[role]
        if kind is None:
            continue  # conditional; handled below
        if kind == _F and n:
            raise ValueError(
                f"[v965] a {lane} clip must not carry a {role.value!r} asset "
                f"(found {n})"
            )
        if kind == _R and not n:
            raise ValueError(
                f"[v965] a {lane} clip is missing its {role.value!r} asset"
            )
        if n and not (lo <= n <= hi):
            raise ValueError(
                f"[v965] a {lane} clip takes {lo}-{hi} {role.value!r} asset(s), "
                f"not {n}"
            )

    if lane == LANE_CHARSWAP:
        _validate_charswap(contract, counts)
    else:
        if contract.swap_mode is not None:
            raise ValueError(
                f"[v965] swap_mode is only for a charswap clip; a {lane} clip "
                f"carries null"
            )
        if contract.swap_max_source_s is not None:
            raise ValueError(
                f"[v965] swap_max_source_s is only for a charswap clip; a "
                f"{lane} clip carries null"
            )

    # The duplicate check is last so its message is not drowned by a shape
    # error. A repeated key means the same file attached twice, which on the
    # Ingredients tab silently deselects it -- clicking an already-selected
    # asset toggles it OFF (HANDOFF rev 822).
    seen = set()
    for entry in contract.assets:
        if entry.key in seen:
            raise ValueError(
                f"[v965] asset key {entry.key!r} appears twice; the same file "
                f"cannot be attached to one clip twice"
            )
        seen.add(entry.key)


def _validate_charswap(contract: ClipContract, counts) -> None:
    """The one lane whose shape depends on a value, not just on the lane."""
    if contract.swap_mode is None:
        raise ValueError(
            "[v965] a charswap clip must declare swap_mode "
            f"({' | '.join(ALLOWED_SWAP_MODES)}); it is not defaulted any more"
        )
    if contract.swap_max_source_s is None:
        raise ValueError(
            "[v965] a charswap clip must carry swap_max_source_s "
            f"(the platform constant is {SWAP_MAX_SOURCE_S})"
        )
    n_start = counts[Role.START_FRAME]
    if contract.swap_mode == "image-led" and n_start != 1:
        raise ValueError(
            "[v965] an image-led charswap clip needs exactly 1 'start_frame' "
            f"asset, not {n_start} -- the still IS the swap input"
        )
    if contract.swap_mode == "video-led" and n_start:
        raise ValueError(
            "[v965] a video-led charswap clip must not carry a 'start_frame' "
            "asset: the source video supplies the motion and the worker never "
            "attaches it, and an asset nobody attaches is one the worker would "
            "have to judge"
        )


# ---------------------------------------------------------------------------
# The accessors. EVERY read of the contract goes through one of these.
#
# Why that matters: `render_method` is read at ~15 sites, each with its own
# hand-written `(getattr(clip, "x", None) or "").strip().lower()` null-guard,
# and the file's own comments name four past incidents caused by one of those
# sites drifting from the others. One accessor cannot drift from itself.
# ---------------------------------------------------------------------------


def read_contract_json(clip) -> Optional[str]:
    """The stored contract string, UNCHANGED, or None if the clip has none.

    Returns the raw string on purpose. It never re-serialises a parsed model,
    because the worker's copy is compared byte-for-byte against what was stored
    and a pydantic round-trip is not guaranteed byte-identical (key order,
    separators, unicode escaping). Byte-equality is the proof that nothing
    rewrote the contract in flight, so it has to be true by construction.
    """
    if getattr(clip, "clip_contract_version", None) is None:
        return None
    raw = getattr(clip, "clip_contract_json", None)
    if raw is None or not str(raw).strip():
        return None
    return raw


def read_contract(clip) -> Optional[ClipContract]:
    """The parsed contract, or None when the clip is pre-contract.

    None means "this clip was made before the contract existed and is not
    judged by it" -- the house NULL-means-legacy convention. It does NOT mean
    "something went wrong": a contract that is present but unreadable raises,
    because a clip that claims a version and cannot produce one is a fault, not
    a legacy row.
    """
    raw = read_contract_json(clip)
    if raw is None:
        return None
    return ClipContract.model_validate_json(raw)


def stamped_clip_filter(clip_cls):
    """The SQL predicate for "this clip is in scope of the contract".

    Takes the mapped class instead of importing it, so this module stays free
    of project imports. The poll gate imports this rather than writing its own
    comparison, so the ONE version predicate lives here -- which is also what
    lets the static check forbid a `clip_contract_version` comparison anywhere
    else without needing a hole cut in it for `main.py`.
    """
    return clip_cls.clip_contract_version.isnot(None)
