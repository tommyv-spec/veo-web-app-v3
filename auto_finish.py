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
