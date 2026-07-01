"""Pure (browser-free) render attribution for the Flow worker.

Clip N owns exactly the renders that appear between its own Generate click and
the next Generate click on the SAME account (submits are sequential per account).
This module holds that state (click log + render ledger) and the bracket math.
flow_worker.py feeds it submit responses + status-poll bodies and writes the
resolved (render_id -> clip) into its existing _PRIMARY_MEDIA_BINDINGS map.

No imports from flow_worker — keep this unit-testable without booting Patchright.
"""
import threading


class RenderAttributor:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self._click_log = {}   # account -> list[{click_at, job_id, clip_index, clip_id}]
        self._ledger = {}      # render_id -> {account, captured_at, create_time, status, batch_id, workflow_id, project_id}
        self._lock = threading.RLock()

    def stamp_click(self, account, job_id, clip_index, clip_id, now):
        """Record a Generate click. `now` = local wall-clock (time.time()) at click."""
        if not account:
            return
        entry = {"click_at": float(now), "job_id": job_id,
                 "clip_index": clip_index, "clip_id": clip_id}
        with self._lock:
            self._click_log.setdefault(account, []).append(entry)
            self._click_log[account].sort(key=lambda e: e["click_at"])

    def click_log_for(self, account):
        with self._lock:
            return [dict(e) for e in self._click_log.get(account, [])]

    def bracket_for(self, account, when):
        """The click-log entry whose bracket [click_at, next_click_at) contains
        `when`. Last entry's bracket is open-ended. Returns a copy or None if
        `when` precedes the first click / the account is unknown."""
        when = float(when)
        with self._lock:
            log = self._click_log.get(account) or []
            owner = None
            for e in log:  # sorted ascending by click_at
                if e["click_at"] <= when:
                    owner = e
                else:
                    break
            return dict(owner) if owner else None

    def observe_render(self, render_id, account, captured_at=None, create_time=None,
                       status=None, batch_id=None, workflow_id=None, project_id=None):
        """Record a render into the ledger and attribute it to the clip whose
        bracket contains it. Returns {job_id, clip_index, clip_id} if attributed,
        else None. Always records the ledger row (used by reconcile) even when
        unattributed or disabled. Idempotent per render_id (later status updates
        upsert; the binding, once found, is stable)."""
        if not render_id:
            return None
        rid = render_id.lower()
        when = captured_at if captured_at is not None else create_time
        with self._lock:
            row = self._ledger.get(rid, {})
            row.update({
                "account": account or row.get("account"),
                "captured_at": captured_at if captured_at is not None else row.get("captured_at"),
                "create_time": create_time if create_time is not None else row.get("create_time"),
                "status": status if status is not None else row.get("status"),
                "batch_id": batch_id if batch_id is not None else row.get("batch_id"),
                "workflow_id": workflow_id if workflow_id is not None else row.get("workflow_id"),
                "project_id": project_id if project_id is not None else row.get("project_id"),
            })
            self._ledger[rid] = row
            if not self.enabled or when is None:
                return None
            owner = self.bracket_for(account, when)
            if not owner:
                return None
            binding = {"job_id": owner["job_id"], "clip_index": owner["clip_index"],
                       "clip_id": owner["clip_id"]}
            row["bound"] = binding
            return dict(binding)

    def renders_for_clip(self, job_id, clip_index):
        """All render ids the ledger has attributed to this (job, clip)."""
        with self._lock:
            out = []
            for rid, row in self._ledger.items():
                b = row.get("bound")
                if b and b["job_id"] == job_id and b["clip_index"] == clip_index:
                    out.append(rid)
            return out
