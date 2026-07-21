import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_http_pull as hp

def test_job_to_prompt_and_refs():
    job = {"id": 7, "prompt": "an apple", "aspect_ratio": "9:16",
           "input_images": [{"url": "http://x/f1", "filename": "r1.png", "role": "the main character", "slot_order": 0}]}
    prompt, refs = hp.job_to_prompt_and_refspec(job)
    assert prompt.startswith("Crea immagine: ")
    assert "vertical 9:16" in prompt
    assert "the main character" in prompt
    assert refs == [{"url": "http://x/f1", "filename": "r1.png"}]

def test_worker_id_is_chatgpt_prefixed():
    assert hp.make_worker_id("HOSTX").startswith("chatgpt-")

def test_status_body_completed():
    assert hp.status_body("completed") == {"status": "completed"}

def test_status_body_failed_carries_error():
    assert hp.status_body("failed", "boom") == {"status": "failed", "error": "boom"}
