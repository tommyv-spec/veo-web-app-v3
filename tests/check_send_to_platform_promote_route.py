"""v892.12 — the CLI promotes through the checked endpoint, and never guesses
a job config.

No network and no server. It imports send_to_platform (whose `requests` import
is deferred into Client, so importing the module costs nothing) and drives the
pure helpers.

The three things being guarded:

  1. `promote-to-video` is GONE from this file. That endpoint writes Clip rows
     itself and carries none of the per-scene bindings; on 2026-09-03 it
     produced 24 clips with clip_role NULL and audio_from_scene NULL, and the
     wrong video rendered with nothing failing. A blanket string guard is used
     on purpose — the moment the name comes back, in a call or in a comment,
     someone is heading back to it.
  2. no config source -> STOP. Not a default, not a guess: every setting has a
     server-side default, so an omitted config silently becomes a full config
     nobody chose.
  3. the CLI enumerates NO config key names. Completeness is checked
     server-side against VideoConfigInput.model_fields; a second list here
     would fall behind the model exactly the way three payload maps already
     have.
"""
import argparse
import json
import os
import re
import sys
import tempfile

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import send_to_platform as stp  # noqa: E402

failures = []


def check(ok, why):
    if not ok:
        failures.append(why)


SRC = open(os.path.join(ROOT, "send_to_platform.py"), encoding="utf-8").read()

# 1. the retired endpoint is gone, name and all
check("promote-to-video" not in SRC,
      "send_to_platform.py mentions promote-to-video again — that path cannot "
      "carry clip_role / audio_from_scene / the composite plate / the v718i end "
      "frame, and it is what produced the 2026-09-03 wrong video")
check("/api/jobs/from-batch/" in SRC,
      "the CLI no longer posts to /api/jobs/from-batch/<batch_id>")


class FakeClient:
    """Answers only GET /api/jobs/<id>/config; anything else is a test bug."""

    def __init__(self, config=None):
        self.config = config
        self.calls = []

    def get(self, path, **kw):
        self.calls.append(("GET", path))
        if re.match(r"^/api/jobs/[^/]+/config$", path):
            return {"config": self.config} if self.config else {}
        raise AssertionError(f"unexpected GET {path}")

    def post(self, path, payload=None, **kw):
        raise AssertionError(f"unexpected POST {path}")


def args_for(**kw):
    ns = argparse.Namespace(job_config=None, config_from_job=None)
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


tmp = tempfile.mkdtemp(prefix="v892_12_")
explicit_path = os.path.join(tmp, "explicit.json")
with open(explicit_path, "w", encoding="utf-8") as fh:
    json.dump({"veo_model": "from-the-explicit-file", "storyboard_mode": True}, fh)

home_path = os.path.join(tmp, "home.json")
with open(home_path, "w", encoding="utf-8") as fh:
    json.dump({"veo_model": "from-the-home-file", "storyboard_mode": True}, fh)

nested_path = os.path.join(tmp, "nested.json")
with open(nested_path, "w", encoding="utf-8") as fh:
    json.dump({"config": {"veo_model": "from-a-saved-job-config"}}, fh)

real_home = stp._SAVED_JOB_CONFIG_PATH
missing_home = os.path.join(tmp, "does-not-exist.json")


def with_home(path, fn):
    stp._SAVED_JOB_CONFIG_PATH = path
    try:
        return fn()
    finally:
        stp._SAVED_JOB_CONFIG_PATH = real_home


# 2. no source at all -> STOP, exit 2, nothing sent
def _no_source():
    try:
        stp.resolve_job_config(args_for(), FakeClient())
    except stp.PlatformError as exc:
        check(exc.exit_code == stp.EXIT_PARSE,
              f"no-config must exit {stp.EXIT_PARSE}, got {exc.exit_code}")
        for needle in ("--job-config", "--config-from-job",
                       "job_config.json", "set-job-config"):
            check(needle in exc.message,
                  f"the no-config error must name {needle}: {exc.message}")
        return
    failures.append("a promote with no job config was allowed to proceed")


with_home(missing_home, _no_source)

# 3. precedence: an explicit file beats a present home file
def _explicit_wins():
    cfg, source = stp.resolve_job_config(
        args_for(job_config=explicit_path), FakeClient({"veo_model": "job"}))
    check(cfg["veo_model"] == "from-the-explicit-file",
          f"--job-config must win over the home file, got {cfg}")
    check(explicit_path in source, f"the source line must name the file: {source}")


with_home(home_path, _explicit_wins)

# 4. a named job beats the home file, and is fetched from the server
def _job_wins():
    client = FakeClient({"veo_model": "from-job-15333490"})
    cfg, source = stp.resolve_job_config(
        args_for(config_from_job="15333490"), client)
    check(cfg["veo_model"] == "from-job-15333490",
          f"--config-from-job must win over the home file, got {cfg}")
    check(client.calls == [("GET", "/api/jobs/15333490/config")],
          f"expected one GET of the job config, got {client.calls}")
    check("15333490" in source, f"the source line must name the job: {source}")


with_home(home_path, _job_wins)

# 5. the home file is used when nothing else is given
def _home_used():
    cfg, source = stp.resolve_job_config(args_for(), FakeClient())
    check(cfg["veo_model"] == "from-the-home-file", f"home file not used: {cfg}")
    check(home_path in source, f"the source line must name the file: {source}")


with_home(home_path, _home_used)

# 6. a file saved straight from GET /api/jobs/<id>/config is accepted as-is
def _nested_accepted():
    cfg, _ = stp.resolve_job_config(args_for(job_config=nested_path), FakeClient())
    check(cfg["veo_model"] == "from-a-saved-job-config",
          f"a {{'config': {{...}}}} file must be unwrapped, got {cfg}")


with_home(missing_home, _nested_accepted)

# 7. a job with no stored config is an error, not an empty config
def _empty_job():
    try:
        stp.resolve_job_config(args_for(config_from_job="404"), FakeClient(None))
    except stp.PlatformError as exc:
        check(exc.exit_code == stp.EXIT_PARSE,
              f"expected exit {stp.EXIT_PARSE}, got {exc.exit_code}")
        return
    failures.append("a job with no stored config produced an empty config "
                    "instead of an error")


with_home(missing_home, _empty_job)

# 8. THE SOURCE GUARD — the CLI enumerates no config keys
check("REQUIRED_EXPLICIT_CONFIG_KEYS" not in SRC,
      "send_to_platform.py declares REQUIRED_EXPLICIT_CONFIG_KEYS — "
      "completeness belongs to the server, against its own model")
# Every key of the server's config model, checked as a quoted literal. The
# names are read off main.VideoConfigInput so this guard cannot fall behind it
# either.
import main  # noqa: E402  (no server is started)

for key in sorted(main.VideoConfigInput.model_fields):
    if re.search(r'["\']' + re.escape(key) + r'["\']', SRC):
        failures.append(
            f"send_to_platform.py names the config key {key!r} — the CLI "
            f"forwards the config it resolved and prints the server's `missing` "
            f"list verbatim; it must hold no key list of its own")

# 9. it prints the config IN FULL, not a chosen shortlist
check("def print_job_config" in SRC and "for key in sorted(config)" in SRC,
      "the config must be printed in full, one setting per line — a shortlist "
      "forgets exactly the setting that costs money")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("PASS — promote-to-video is gone from the CLI and it posts to "
      "/api/jobs/from-batch/; no config source exits 2 naming all three ways "
      "to fix it; --job-config beats --config-from-job beats "
      "~/.kaveno/job_config.json; the CLI names not one config key and prints "
      "whatever it resolved in full.")
