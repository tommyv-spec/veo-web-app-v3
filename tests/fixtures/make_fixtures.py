"""Generate 4 forced-alignment test fixtures using pyttsx3 (offline TTS).

Run once: python code/tests/fixtures/make_fixtures.py
Outputs 4 .wav + 4 .json files alongside this script.
"""
import json
from pathlib import Path

import pyttsx3

OUT = Path(__file__).parent

FIXTURES = {
    "align_clean": {
        "script": "blood flow is the silent key to lasting performance",
        "drop_word_idx": None,
        "insert_filler_after_idx": None,
    },
    "align_dropped": {
        "script": "the saffron stained the milk a deep golden hue",
        "drop_word_idx": 2,  # "stained" dropped
        "insert_filler_after_idx": None,
    },
    "align_filler": {
        "script": "I tried it for fourteen days and saw real change",
        "drop_word_idx": None,
        "insert_filler_after_idx": 5,  # breath inserted after "days"
    },
    "align_rare": {
        "script": "the laureth sulfate reacts with the phenylethyl alcohol",
        "drop_word_idx": None,
        "insert_filler_after_idx": None,
    },
}


def synth(text: str, out_path: Path) -> None:
    eng = pyttsx3.init()
    eng.setProperty("rate", 180)
    eng.save_to_file(text, str(out_path))
    eng.runAndWait()


def render_fixture(name: str, cfg: dict) -> None:
    words = cfg["script"].split()
    spoken = list(words)
    if cfg["drop_word_idx"] is not None:
        del spoken[cfg["drop_word_idx"]]
    if cfg["insert_filler_after_idx"] is not None:
        spoken.insert(cfg["insert_filler_after_idx"] + 1, "uhh")
    synth(" ".join(spoken), OUT / f"{name}.wav")
    (OUT / f"{name}.json").write_text(json.dumps({
        "script": cfg["script"],
        "spoken_words": spoken,
        "drop_word_idx": cfg["drop_word_idx"],
        "insert_filler_after_idx": cfg["insert_filler_after_idx"],
    }, indent=2))


if __name__ == "__main__":
    for n, c in FIXTURES.items():
        render_fixture(n, c)
        print(f"wrote {n}")
