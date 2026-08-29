# tests/test_v953_skip_start_trim.py
#
# v953 — the start-trim decision used to live inline in main.py's export body,
# ~300 lines deep in an endpoint no test could reach. That is how it got changed
# twice in one week with nobody able to pin the behaviour: turned off on the
# paddleboard build at 2026-08-27 14:13, undone by v947.3 at 14:53 after the
# blanket trim cut real words out of speech ("THREE rules" -> "Rules",
# "KORella" -> "Ella", "reach you" -> "read").
#
# The first test is the one that matters for THIS commit: the extracted function
# must be behaviourally identical to the block it replaced, for every input.
# Only once that is nailed down is flipping the default a one-variable change.

import itertools
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from finishing_models import ExportSettings, skip_start_trim


def _old_inline(smart_trim, has_lineup, pos, clip_index, cut_scene_first_clips):
    """main.py's block, verbatim, before v953 extracted it."""
    out = False
    if smart_trim:
        if has_lineup:
            out = (pos == 0)
        else:
            out = (clip_index == 0 or clip_index in cut_scene_first_clips)
    return out


def test_extraction_is_identical_to_the_old_block_at_the_shipped_default():
    """Exhaustive over every combination that can occur. With
    trim_cut_scene_starts=False (this commit's default) the new function must
    agree with the old code on every single input."""
    cut_sets = [set(), {1}, {0, 1}, {2, 5}, {0, 1, 2, 3}]
    checked = 0
    for smart, lineup, pos, idx, cuts in itertools.product(
            (True, False), (True, False), range(4), range(4), cut_sets):
        got = skip_start_trim(smart_trim=smart, trim_cut_scene_starts=False,
                              has_lineup=lineup, pos=pos, clip_index=idx,
                              cut_scene_first_clips=cuts)
        want = _old_inline(smart, lineup, pos, idx, cuts)
        assert got == want, (smart, lineup, pos, idx, cuts, got, want)
        checked += 1
    assert checked == 2 * 2 * 4 * 4 * len(cut_sets)


def test_the_shipped_default_changes_nothing():
    assert ExportSettings().trim_cut_scene_starts is False
    assert ExportSettings().smart_trim is True


def test_clip_zero_stays_protected_either_way():
    """The half of smart_trim nobody disputes: never shave the opening frames of
    the finished video."""
    for flag in (False, True):
        assert skip_start_trim(smart_trim=True, trim_cut_scene_starts=flag,
                               has_lineup=False, pos=0, clip_index=0,
                               cut_scene_first_clips=set()) is True


def test_turning_the_flag_on_trims_cut_scene_starts():
    """The behaviour the flag exists to enable. Clip 3 opens a `cut` scene."""
    kw = dict(smart_trim=True, has_lineup=False, pos=3, clip_index=3,
              cut_scene_first_clips={3})
    assert skip_start_trim(trim_cut_scene_starts=False, **kw) is True   # old
    assert skip_start_trim(trim_cut_scene_starts=True, **kw) is False   # new


def test_smart_trim_off_still_beats_everything():
    for flag in (False, True):
        assert skip_start_trim(smart_trim=False, trim_cut_scene_starts=flag,
                               has_lineup=False, pos=0, clip_index=0,
                               cut_scene_first_clips={0}) is False


def test_the_lineup_branch_is_untouched_by_the_new_flag():
    """A lineup export answers 'first' by POSITION and has never consulted the
    cut-scene set. It therefore already ships the behaviour the flag turns on for
    everyone else, and must not change."""
    for flag in (False, True):
        assert skip_start_trim(smart_trim=True, trim_cut_scene_starts=flag,
                               has_lineup=True, pos=0, clip_index=9,
                               cut_scene_first_clips={9}) is True
        assert skip_start_trim(smart_trim=True, trim_cut_scene_starts=flag,
                               has_lineup=True, pos=2, clip_index=0,
                               cut_scene_first_clips={0}) is False


def test_the_field_is_declarable_from_a_build():
    """v947 validates export_* against the REAL model, so a new field becomes
    declarable with no parser edit. Prove it rather than assume it."""
    assert "trim_cut_scene_starts" in ExportSettings.model_fields
    assert ExportSettings(trim_cut_scene_starts=True).trim_cut_scene_starts is True
