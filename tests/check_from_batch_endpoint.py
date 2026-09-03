"""v892.12 — the from-batch endpoint's contract, checked at the source.

No server is started and no job is created. What this file guards is the
handful of properties that make the endpoint safe, and each one is a property
you cannot see by reading the happy path:

  * the verification runs BEFORE the background task is spawned. If those two
    ever swap order the check still passes on the log and still races the
    render — the wrong job can be claimed by a worker while the check is
    running. That is precisely why `spawn_setup=False` exists.
  * `_SetupContext` never becomes serialisable. It carries `api_keys_data`, so
    a pydantic base class or a `response_model=` mention would be a path for
    API keys to leave the server.
  * `spawn_setup` keeps its default. A mandatory argument is an API break, and
    the caller that breaks is the one that cannot complain.
  * the endpoint holds no hand-written config key list. Completeness is
    computed from VideoConfigInput itself.
"""
import inspect
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import dataclasses  # noqa: E402
from pydantic import BaseModel  # noqa: E402

import main  # noqa: E402

failures = []


def check(ok, why):
    if not ok:
        failures.append(why)


# 1. the route exists, exactly once
routes = [r.path for r in main.app.routes
          if getattr(r, "path", "") == "/api/jobs/from-batch/{batch_id}"]
check(len(routes) == 1,
      f"expected exactly one /api/jobs/from-batch/{{batch_id}} route, got {routes}")

# 2. _SetupContext stays a plain dataclass and never reaches a response
check(dataclasses.is_dataclass(main._SetupContext),
      "_SetupContext must be a plain dataclass")
check(not issubclass(main._SetupContext, BaseModel),
      "_SetupContext must NOT be a pydantic model — it carries api_keys_data, "
      "and a serialisable response object is a path for keys to leave")
main_src = inspect.getsource(main)
for m in re.finditer(r"response_model\s*=\s*([A-Za-z_0-9\[\]., ]+)", main_src):
    if "_SetupContext" in m.group(1):
        failures.append(f"_SetupContext appears in `{m.group(0)}`")

# 3. spawn_setup defaults to True — no existing caller changes behaviour
impl = inspect.signature(main._create_job_impl).parameters
check("spawn_setup" in impl, "_create_job_impl has no spawn_setup parameter")
check(impl["spawn_setup"].default is True,
      f"spawn_setup must default to True, got {impl['spawn_setup'].default!r} — "
      f"a mandatory argument is an API break")

# 4. create_job is untouched: same response model, same three arguments
create_job_src = inspect.getsource(main.create_job)
check("spawn_setup" not in create_job_src,
      "create_job must keep calling _create_job_impl with the default")

# 5. THE ORDER. Verify, then spawn — never the other way round.
handler_src = inspect.getsource(main.create_job_from_batch)
i_verify = handler_src.find("verify_promoted_clips(")
i_spawn = handler_src.find("asyncio.create_task(")
check(i_verify != -1, "the handler does not call verify_promoted_clips")
check(i_spawn != -1, "the handler never spawns the background setup")
check(-1 < i_verify < i_spawn,
      "the handler spawns the background setup BEFORE verifying the rows — "
      "setup ends in queued_for_flow and a worker can claim within seconds, so "
      "a check after the spawn races the render it is meant to prevent")
check("spawn_setup=False" in handler_src,
      "the handler must call _create_job_impl with spawn_setup=False")

# 6. a failed verification destroys the job
check("[v892.12 VERIFY FAIL]" in handler_src,
      "problems must be logged with the [v892.12 VERIFY FAIL] prefix")
check("db.delete(dead)" in handler_src,
      "a failed verification must delete the job row, not just log")
check("promoted_video_job_id = None" in handler_src,
      "a failed verification must un-stamp the batch")

# 7. the TEMP diagnostic is present and marked (root CLAUDE.md §2)
check("[TEMP v892.12 FROM-BATCH]" in handler_src,
      "the runtime change carries no TEMP diagnostic line")

# 8. no hand-written config key list anywhere on this path
check("VideoConfigInput.model_fields" in handler_src,
      "completeness must be computed from the model, not from a key list")
for key in sorted(main.VideoConfigInput.model_fields):
    if key == "storyboard_mode":
        continue          # named on purpose: this path REFUSES it when false
    if re.search(r'["\']' + re.escape(key) + r'["\']', handler_src):
        failures.append(
            f"the handler names the config key {key!r} — the required set is "
            f"computed from VideoConfigInput.model_fields, and a second list "
            f"beside it is the drift this whole design avoids")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("PASS — /api/jobs/from-batch/{batch_id} registered once; verification "
      "runs BEFORE the setup task is spawned; a failure deletes the job and "
      "un-stamps the batch; _SetupContext is a plain dataclass absent from "
      "every response_model; spawn_setup still defaults to True; no config key "
      "list on the path.")
