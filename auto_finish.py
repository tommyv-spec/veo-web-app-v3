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


def export_modal_defaults(spec):
    """v951 — the settings the Export dialog should OPEN on for this job.

    The dialog used to open on localStorage, i.e. whatever this browser picked
    last, on any video, in any lane. This returns the video's own answer
    instead: every ExportSettings field at its model default, with the build's
    declared `export_*` folded on top, plus `declared` naming the keys the
    build actually decided (the UI shows those, so the choice stays checkable).

    Read-only and total: a corrupt or absent spec degrades to plain model
    defaults rather than raising. Opening a dialog is not where a bad build
    should surface — import already fails closed on that
    (image_platform._finishing_validate_prefixed).
    """
    from finishing_models import ExportSettings
    settings = ExportSettings().model_dump()
    declared = {}
    if isinstance(spec, dict):
        raw = spec.get("export")
        if isinstance(raw, dict):
            # Only keys the model actually has. The import-time validator
            # already rejects unknown ones; this is the belt for a spec that
            # was stored by an older, looser build of the parser.
            declared = {k: v for k, v in raw.items()
                        if k in ExportSettings.model_fields}
    settings.update(declared)
    return {"settings": settings, "declared": sorted(declared)}
