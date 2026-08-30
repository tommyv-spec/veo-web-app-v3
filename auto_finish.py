"""v947 — pure decisions for the auto-finish chain.

The build's ## Finishing may declare `auto_finish: on` plus export_*/autoedit_*
settings. When the LAST clip of the job is approved, main.py queues the export
with the declared settings; when that export completes, it queues the auto-edit
with the declared settings. These are the decisions; main.py applies them to
rows. No DB imports here — mirror of export_queue.py / autoedit_queue.py.
"""


def auto_finish_on(spec) -> bool:
    """True only when the job's declared finishing says `auto_finish: on`."""
    return bool(spec) and str(spec.get("auto_finish", "off")).lower() == "on"


def all_clips_approved(approval_statuses) -> bool:
    """True when the job HAS clips and every clip row is approved.

    A None status (undecided), 'rejected', or an empty job all mean NO —
    the chain fires only on a fully-approved, non-empty clip set.
    """
    statuses = list(approval_statuses)
    return bool(statuses) and all(s == "approved" for s in statuses)


def derive_export_defaults(req_dict, spec, request_was_explicit):
    """Fold the build's declared export_* finishing into an export request.

    Same rev-459 inheritance shape as derive_autoedit_defaults (main.py):
    the declaration supplies DEFAULTS; a field the caller explicitly sent
    always wins. `request_was_explicit` is pydantic's model_fields_set, so a
    value that merely equals the default is NOT a choice. Returns a NEW dict.
    """
    out = dict(req_dict or {})
    for k, v in ((spec or {}).get("export") or {}).items():
        if k not in request_was_explicit:
            out[k] = v
    return out


def derive_lane_defaults(*, all_charswap, any_source_audio, has_speech):
    """v957 — (lane, derived-settings) from what the job factually is.

    The v951 authoring table derived a lane's export settings from the
    build's Storyboard, but only when the build DECLARED them. These are the
    same answers computed from the job's own clip rows, so an undeclared job
    still opens the Export dialog on values that fit the video.

    Deliberately conservative: only the all-charswap lanes derive anything.
    A spoken or mixed job returns {} — turning ON whisper silence removal by
    derivation would change behaviour for the 300+ spoken builds that predate
    this rule (the measure-the-population lesson). Whisper stays a declared
    choice. Modal-only by design: _maybe_auto_finish_export keeps reading
    declared settings only (v951 decision, unchanged).
    """
    if all_charswap and not has_speech:
        lane = "charswap-music" if any_source_audio else "charswap-silent"
        return lane, {
            # cut-boundary head trim must actually run: smart_trim would
            # skip the start trim on cut scenes (v953) and let Veo lead-in
            # frames through
            "smart_trim": False,
            "frames_to_cut_start": 7,
            # silence removal has nothing to keep on a no-speech clip, and
            # whisper on music keeps NOTHING (v951's measured wrong dialog)
            "remove_silence": False,
            # speeding a source-audio clip re-times the music; a silent one
            # gains nothing
            "playback_speed": 1.0,
        }
    # source audio alongside speech means the job is not one lane — that is
    # the `mixed` fallthrough below, not `spoken`
    if has_speech and not all_charswap and not any_source_audio:
        return "spoken", {}
    if not has_speech and not all_charswap and not any_source_audio:
        return "unknown", {}
    return "mixed", {}


def export_modal_defaults(spec, job_facts=None):
    """v951 + v957 — the settings the Export dialog should OPEN on.

    Precedence (lowest first): model defaults < derived-from-lane (v957,
    only when job_facts given) < the build's declared export_* (v951).
    Returns settings + declared + derived (keys the lane supplied that no
    declaration overrode) + lane (None when no facts were given).

    Read-only and total: corrupt/absent spec or facts degrade toward plain
    model defaults rather than raising.
    """
    from finishing_models import ExportSettings
    settings = ExportSettings().model_dump()

    lane = None
    derived_applied = []
    if isinstance(job_facts, dict):
        lane, derived = derive_lane_defaults(
            all_charswap=bool(job_facts.get("all_charswap")),
            any_source_audio=bool(job_facts.get("any_source_audio")),
            has_speech=bool(job_facts.get("has_speech")))
        for k, v in derived.items():
            if k in ExportSettings.model_fields:
                settings[k] = v
                derived_applied.append(k)

    declared = {}
    if isinstance(spec, dict):
        raw = spec.get("export")
        if isinstance(raw, dict):
            declared = {k: v for k, v in raw.items()
                        if k in ExportSettings.model_fields}
    settings.update(declared)
    # a key both derived and declared reports as declared only — the operator
    # reads ONE source of truth per field
    derived_applied = [k for k in derived_applied if k not in declared]
    return {"settings": settings, "declared": sorted(declared),
            "derived": sorted(derived_applied), "lane": lane}
