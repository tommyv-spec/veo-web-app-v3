import sys, os, json, tempfile, shutil
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_image_worker as w


def _job(tmp, jid="node_1", model="chatgpt"):
    out = os.path.join(tmp, jid); os.makedirs(out, exist_ok=True)
    job = {"id": jid, "prompt": "an apple", "input_images": [],
           "output_dir": out, "aspect_ratio": "9:16", "resolution": "1K",
           "model": model, "variants": 4}
    jp = os.path.join(tmp, jid + ".json")
    open(jp, "w").write(json.dumps(job))
    return jp, job, out

def test_claim_only_chatgpt_model(tmp_path):
    assert w._is_chatgpt_job({"model": "chatgpt"}) is True
    assert w._is_chatgpt_job({"model": "nano_banana_2"}) is False
    assert w._is_chatgpt_job({}) is False

def test_process_one_writes_variant_and_done(tmp_path, monkeypatch):
    tmp = str(tmp_path)
    jp, job, out = _job(tmp)
    def fake_generate(page, prompt, ref_paths, out_path, **k):
        open(out_path, "wb").write(b"\x89PNG\r\n\x1a\nFAKE"); return out_path
    monkeypatch.setattr(w, "generate", fake_generate)
    w._process_platform_job(page=None, job_path=jp, job=job)
    assert os.path.exists(os.path.join(out, "variant_1.png"))
    assert not os.path.exists(os.path.join(out, "variant_2.png"))
    donep = jp.replace(".json", ".done.json")
    assert os.path.exists(donep)
    d = json.load(open(donep))
    assert d["status"] == "completed" and d["output_paths"] == ["variant_1.png"]

def test_process_failure_writes_failed_done(tmp_path, monkeypatch):
    tmp = str(tmp_path)
    jp, job, out = _job(tmp)
    def boom(page, prompt, ref_paths, out_path, **k):
        raise TimeoutError("no image")
    monkeypatch.setattr(w, "generate", boom)
    w._process_platform_job(page=None, job_path=jp, job=job)
    d = json.load(open(jp.replace(".json", ".done.json")))
    assert d["status"] == "failed" and "no image" in (d["error"] or "")

def test_scan_skips_already_done(tmp_path):
    tmp = str(tmp_path)
    jp, job, out = _job(tmp)
    open(jp.replace(".json", ".done.json"), "w").write("{}")
    assert jp not in w._scan_pending(tmp)

def test_scan_skips_non_chatgpt(tmp_path):
    tmp = str(tmp_path)
    jp, job, out = _job(tmp, model="nano_banana_2")
    assert w._scan_pending(tmp) == []
