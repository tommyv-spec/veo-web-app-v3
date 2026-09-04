"""v960.1 — the ✂️ Auto-Edit card's caption-style dropdown defaults to the
template the BUILD declared, ahead of whatever the last run happened to use.

Why this needs a test at all: the card sends `template` explicitly on every
click, so the server's v944 derive can never apply a declared `captions:` for
it. The dropdown's default is the ONLY place the declaration can land, and it
used to read the last run first — so a job declaring `captions: garnissa` whose
first run went out as korella would offer korella forever.

The precedence under test, highest first:
  1. what the operator currently has selected on screen (`pickedTemplate`)
  2. the build's declared template          <- v960.1 added this rung
  3. the last run's template
  4. the server's default, then the first name

Pure rules re-implemented here and asserted against the shipped JS by static
read — there is no JS runtime in this test suite.

Run: python tests/check_v960_1_declared_template_default.py   (from code/)
"""
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NAMES = ["garnissa", "korella", "korella2line", "word-focus"]


def chosen(picked, declared, saved, default):
    """The dropdown rule, mirrored from index.html."""
    dec = declared if (declared and declared in NAMES) else None
    sav = saved if (saved and saved in NAMES) else None
    if picked and picked in NAMES:
        return picked
    return dec or sav or default or (NAMES[0] if NAMES else "")


# the case this rule exists for: build says garnissa, the only run was korella
assert chosen(None, "garnissa", "korella", "korella") == "garnissa"
# nothing declared -> unchanged from before v960.1
assert chosen(None, None, "korella", "korella") == "korella"
# nothing declared and never run -> the server default
assert chosen(None, None, None, "korella") == "korella"
# the operator's on-screen pick outranks the declaration
assert chosen("word-focus", "garnissa", "korella", "korella") == "word-focus"
# a declared template the server does not have is ignored, not rendered
assert chosen(None, "no-such-style", "korella", "korella") == "korella"
# declared and never run
assert chosen(None, "garnissa", None, "korella") == "garnissa"
# an on-screen pick that is no longer a known name falls through
assert chosen("retired-style", "garnissa", "korella", "korella") == "garnissa"
print("OK precedence: pick > declared > last run > default")

# `captions: none` and a job that declared nothing both mean "no declared
# template" — the helper must return null for both, never the string "none".
src = open(os.path.join(ROOT, "static", "index.html"), encoding="utf-8").read()
helper = src[src.find("async function _autoEditGetDeclaredTemplate("):]
helper = helper[:helper.find("\n        }") + 10]
assert "d.finishing_spec && d.finishing_spec.captions" in helper
assert "(c && c !== 'none') ? c : null" in helper, \
    "`captions: none` must read as no declared template, not as the style 'none'"
assert "if (!r.ok) return null" in helper, "an older server must degrade to null, not throw"
assert "catch (e)" in helper and "return null" in helper
print("OK helper: none / absent / old server / network error all read as null")

# the card actually uses it, and fetches it before building the dropdown
card = src[src.find("async function renderAutoEditCard("):]
card = card[:card.find("\n        async function ")] if "\n        async function " in card else card
i_fetch = card.find("const declaredCaptionTemplate = await _autoEditGetDeclaredTemplate(jobId);")
i_use = card.find("const declaredTemplate = declaredCaptionTemplate")
i_chosen = card.find("const chosen = pickedTemplate")
assert i_fetch != -1, "the card never reads the declared template"
assert i_use != -1, "the declared template is read but never used"
assert i_fetch < i_use < i_chosen, "it must be fetched before the dropdown is built"
assert re.search(r"\(declaredTemplate \|\| savedTemplate \|\| tpl\.default", card), \
    "declared must sit AHEAD of the last run in the fallback chain"
# the mid-fetch job switch guard the rest of this function uses
guard = card[i_fetch:i_fetch + 260]
assert "selectedJobId !== jobId" in guard, \
    "an await inside the card needs the switched-jobs guard the others have"
print("OK card: fetched first, guarded, and ranked above the last run")

# the read endpoint exists, is a GET, and has no side effects
main = open(os.path.join(ROOT, "main.py"), encoding="utf-8").read()
i_get = main.find('@app.get("/api/jobs/{job_id}/finishing")')
i_post = main.find('@app.post("/api/jobs/{job_id}/finishing")')
assert i_get != -1, "no GET for the declared finishing"
assert i_get < i_post, "the read companion should sit beside its POST"
body = main[i_get:i_post]
assert "_job_finishing_spec(job)" in body
assert "get_user_job(db, job_id, current_user)" in body, "the read must be access-checked"
for banned in ("db.commit()", "_maybe_auto_finish", "job.finishing_spec ="):
    assert banned not in body, f"the GET must not {banned} — it is read-only"
print("OK endpoint: GET, access-checked, no writes and no auto-finish trigger")

print("ALL OK check_v960_1_declared_template_default")
