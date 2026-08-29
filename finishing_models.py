"""The two finish-request models, in one importable home.

Moved out of main.py (2026-08-26, v947) so image_platform.py can validate the
build markdown's declared ## Finishing export_*/autoedit_* fields against the
REAL models instead of a hand-maintained copy that would drift (the v892.2
lesson). main.py imports them from here; behavior is unchanged.
"""
from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field


class ExportSettings(BaseModel):
    frames_to_cut_start: int = Field(default=7, ge=0, le=30)
    frames_to_cut_end: int = Field(default=7, ge=0, le=30)
    smart_trim: bool = True  # Master switch for start-trim protection (see below)
    # v953 — split out of smart_trim, which meant two things at once: "never trim
    # the FIRST clip of the video" and "never trim the first clip of a
    # `transition: cut` scene". Only the first is uncontroversial. The second was
    # measured wrong: every scene in the paddleboard build declares `cut`, so ALL
    # 11 clips landed in the skip set and 4 frames of a WRONG scene shipped at the
    # 38.5s boundary. Five of five builds that ever declared smart_trim set it
    # false to escape exactly this.
    #
    # False = the old behaviour (skip the trim on cut-scene starts too).
    # True  = trim them; clip 0 stays protected either way.
    #
    # Landing at False first, so the extracted decision below is provably
    # identical to the inline block it replaced. Flipping the default is a
    # separate decision with its own evidence — see v947.3, which had to undo a
    # blanket start-trim after it cut real words out of speech.
    trim_cut_scene_starts: bool = False
    remove_silence: bool = False
    silence_mode: str = "energy"  # "energy" = ffmpeg silencedetect, "whisper" = speech-based detection
    silence_trigger: float = Field(default=1.5, ge=0.3, le=5.0)   # Gaps >= this are trimmed (seconds)
    silence_keep: float = Field(default=0.3, ge=0.0, le=2.0)       # Silence to preserve at each cut (seconds)
    silence_threshold: float = Field(default=0.75, ge=0.1, le=1.0) # VAD confidence: higher = only clear speech kept
    # v948 — post-concat silence-hole sweep. The per-clip VAD above trims each
    # clip's OWN edges; it cannot see a pause that only exists once the clips
    # are stacked, and it cannot see a pause in the middle of a clip. Those
    # holes survive into the assembled final. Set this (e.g. 0.9) and every
    # silence >= this many seconds in the FINISHED file is cut down to a ~0.3s
    # breath. None/absent = the sweep never runs and the export is
    # byte-identical to before v948.
    max_silence_s: Optional[float] = None
    # Individual audio enhancement toggles
    remove_laughter: bool = False  # noisereduce (treats laughter as noise)
    denoise_strength: float = Field(default=0.75, ge=0.0, le=1.0)
    apply_deepfilter: bool = False  # DeepFilterNet (removes hiss/static)
    apply_voice_filter: bool = False  # Compressor, gate, limiter
    apply_loudnorm: bool = False  # EBU R128 -16 LUFS
    # Master audio alignment (assemble jobs only)
    master_audio_filename: Optional[str] = None  # If set, align clips to this master audio
    max_clip_speed: float = Field(default=1.5, ge=0.9, le=5.0)  # Max speed multiplier for clip alignment (0.9=slight slowdown, 5.0=very fast)
    min_gap_for_black: float = Field(default=2.0, ge=0.0, le=10.0)  # Gaps shorter than this (seconds) are filled by extending the previous clip instead of black
    # v888 music bed — lay a track under the finished cut. Distinct from
    # master_audio_filename above, which ALIGNS clips to a spoken master and
    # routes to a different export path entirely. This is a score, not a guide.
    # music_start_s must be the beatplan's `music_source_start`, or the bar
    # phase is wrong and every cut sits off the grid even with perfect lengths.
    music_filename: Optional[str] = None
    music_start_s: float = Field(default=0.0, ge=0.0)
    music_gain_db: float = Field(default=0.0, ge=-40.0, le=10.0)
    music_mode: str = "replace"  # "replace" (silent builds) | "mix" (duck under dialogue)
    # v890 beat alignment. OFF => the md's authored target_duration_s drives the
    # cut, untouched (operator 2026-08-04: "default is keep my timings if any in
    # the md"). ON => each authored cut NUDGES to the nearest strong beat.
    beat_align: bool = False
    beat_tol_beats: float = Field(default=0.6, ge=0.1, le=2.0)  # max nudge, in beats
    # v890.6 — the beat_drop_aligner_v5 controls.
    #   snap  = keep the authored clip lengths, nudge each cut to a beat.
    #   solve = the MUSIC picks every length inside [min,max], clips accelerate
    #           into the drop, and beat_drop_clip lands ON it. Authored lengths
    #           are discarded, so this is for montages, not narrative builds.
    beat_mode: str = "snap"
    beat_min_s: float = Field(default=0.5, ge=0.1, le=10.0)
    beat_max_s: float = Field(default=2.0, ge=0.2, le=20.0)
    beat_drop_clip: Optional[int] = None     # 1-based clip that starts on the drop
    beat_drop_rank: int = Field(default=1, ge=1, le=8)
    beat_drop_time: Optional[float] = None   # exact seconds, bypasses detection
    beat_pre_drop_speed: float = Field(default=1.0, ge=0.5, le=3.0)
    beat_post_drop_speed: float = Field(default=1.0, ge=0.5, le=3.0)
    beat_clip_speed: float = Field(default=1.0, ge=0.5, le=3.0)   # global multiplier
    beat_beats_per_bar: int = Field(default=4, ge=2, le=12)
    # v5 --pin-clip: {"3": 2.47} forces clip 3 to ~2.47s, beat-snapped, ignoring
    # min/max for that clip only. Does not backtrack (same limit as v5).
    beat_pins: Optional[dict] = None
    # Transitions (assemble jobs only)
    transition: str = "none"  # xfade transition type: none, fade, fadeblack, fadewhite, slideleft, slideright, slideup, slidedown, dissolve, circlecrop, wipeleft, wiperight, smoothleft, smoothright, radial, zoomin, pixelize
    transition_duration: float = Field(default=0.5, ge=0.2, le=1.5)
    # Legacy (backwards compatibility)
    playback_speed: float = Field(default=1.0, ge=1.0, le=1.5)  # 1.0 = normal, up to 1.5×
    enhance_audio: bool = False


class AutoEditRequest(BaseModel):
    template: str = "korella"
    placement: str = "dynamic"     # dynamic|constant
    offset: Optional[float] = None
    trim_start_s: float = 0.0
    trim_end_s: float = 0.0
    pip_enabled: bool = True
    captions_enabled: bool = True
    chroma_similarity: float = 0.10
    chroma_blend: float = 0.02
    music_filename: Optional[str] = None
    music_db: float = -20.0
    # v947.2 — "voice" (default: the talking-head enhancement chain) or "off"
    # (source-original / music-bed videos: the export's audio passes through
    # untouched — the voice chain's denoiser treats music as noise and guts it).
    # Literal, not str: the closed value set belongs on the MODEL, so a bad
    # value dies at import/parse AND at the endpoint, not at queue time.
    #
    # v948.2 — "level" is the middle setting: everything the voice chain does
    # EXCEPT the DeepFilter denoiser (voice EQ, compressor, limiter, two-pass
    # loudness). It exists because "off" turns off FOUR things at once, and a
    # v948-swept export needs exactly one of them gone. Measured on job
    # 29d45418: the denoiser crushes quiet room tone below the silence floor and
    # re-creates the holes the sweep just removed, so the sweep forces "off" —
    # and "off" also drops the loudness pass, landing the final at -25.1 LUFS
    # against a -14.3 LUFS published reference. Neither existing value can ship
    # that video.
    audio_enhance: Literal["voice", "off", "level"] = "voice"
    hook_corner: Optional[float] = None
    hook_bg: Optional[str] = None
    # v944 — the text overlay, normally DERIVED from the job's declared
    # finishing. Sending it explicitly overrides that, which is how a job
    # imported before v944 (nothing declared, nothing to derive) can still be
    # re-finished with an overlay without re-importing the build.
    overlay_spec: Optional[Dict[str, Any]] = None


def skip_start_trim(*, smart_trim, trim_cut_scene_starts, has_lineup, pos,
                    clip_index, cut_scene_first_clips):
    """Should this clip KEEP its head frames instead of losing frames_to_cut_start?

    v953 — extracted verbatim from the inline block in main.py's export body. It
    lived 300 lines deep inside an endpoint no test could reach, which is how a
    behaviour nobody could pin got changed twice in one week (turned off on
    2026-08-27 14:13, undone by v947.3 at 14:53 after it cut real words out of
    speech: "THREE rules" -> "Rules", "KORella" -> "Ella").

    smart_trim protects the FIRST clip of the finished video and nothing else.
    trim_cut_scene_starts answers the separate question about a clip that merely
    OPENS a `transition: cut` scene.

    The lineup branch has always answered "first" differently — position in the
    operator's chosen order, never clip_index — and has never consulted the
    cut-scene set at all. So a lineup export ALREADY ships the behaviour
    trim_cut_scene_starts=True turns on for everyone else. Preserved verbatim;
    do not "tidy" the two branches together.
    """
    if not smart_trim:
        return False
    if has_lineup:
        return pos == 0
    if clip_index == 0:
        return True
    return (not trim_cut_scene_starts) and clip_index in cut_scene_first_clips
