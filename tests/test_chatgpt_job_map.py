import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_job_map as m


def test_aspect_phrase_vertical():
    assert m.aspect_phrase("9:16") == "vertical 9:16"

def test_aspect_phrase_horizontal():
    assert m.aspect_phrase("16:9") == "horizontal 16:9"

def test_aspect_phrase_unknown_defaults_empty():
    assert m.aspect_phrase("") == ""

def test_build_prompt_prepends_trigger_and_appends_aspect():
    job = {"prompt": "a red apple on a counter", "aspect_ratio": "9:16", "input_images": []}
    p = m.build_prompt(job)
    assert p.startswith("Crea immagine: ")
    assert "a red apple on a counter" in p
    assert "vertical 9:16" in p

def test_build_prompt_adds_one_line_per_ref_role():
    job = {"prompt": "scene", "aspect_ratio": "9:16",
           "input_images": [{"path": "/a.png", "role": "the main character", "slot_order": 0},
                            {"path": "/b.png", "role": "the salvora bottle", "slot_order": 1}]}
    p = m.build_prompt(job)
    assert "use the uploaded reference for the main character" in p
    assert "use the uploaded reference for the salvora bottle" in p

def test_ref_paths_sorted_by_slot_order():
    job = {"input_images": [{"path": "/b.png", "slot_order": 1},
                            {"path": "/a.png", "slot_order": 0}]}
    assert m.ref_paths(job) == ["/a.png", "/b.png"]

def test_ref_paths_empty_when_no_inputs():
    assert m.ref_paths({"input_images": []}) == []
    assert m.ref_paths({}) == []

def test_done_payload_shape():
    d = m.done_payload("node_5", "completed", ["variant_1.png"], None)
    assert d == {"id": "node_5", "status": "completed",
                 "output_paths": ["variant_1.png"], "error": None}

def test_done_payload_failed_carries_error():
    d = m.done_payload("node_5", "failed", [], "timeout")
    assert d["status"] == "failed" and d["error"] == "timeout" and d["output_paths"] == []
