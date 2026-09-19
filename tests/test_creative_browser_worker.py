from __future__ import annotations

import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest


STATIC = Path(__file__).resolve().parents[1] / "static"
if str(STATIC) not in sys.path:
    sys.path.insert(0, str(STATIC))

import creative_browser_worker as worker


TEST_ROOT = Path(__file__).resolve().parents[2] / "output" / "creative_browser_worker_unit_tests"
LOCAL_TEST_ROOT = Path(__file__).resolve().parents[1] / "data" / "creative_browser_worker_unit_tests"


def _case(name: str) -> Path:
    path = TEST_ROOT / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _local_case(name: str) -> Path:
    path = LOCAL_TEST_ROOT / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _manifest(root: Path, text: str = "source") -> Path:
    task_path = root / "task.md"
    task_path.write_text("task", encoding="utf-8")
    parent_path = root / "parent-build.md"
    parent_path.write_text("parent", encoding="utf-8")
    file_path = root / "decoded_parent.md"
    file_path.write_text(text, encoding="utf-8")
    task_data = task_path.read_bytes()
    parent_data = parent_path.read_bytes()
    data = file_path.read_bytes()
    manifest = {
        "schema": 2,
        "method": "step-up",
        "source_contract": {
            "method": "step-up",
            "parent_build": "parent-build.md",
            "parent_decode": "decoded_parent.md",
            "second_source_decode": None,
        },
        "message": "read it",
        "required_headings": [
            "### Classification",
            "### Selected direction",
            "### Challenge test",
        ],
        "task": "task.md",
        "attachment_count": 3,
        "files": [
            {
                "path": "task.md",
                "role": "task",
                "bytes": len(task_data),
                "sha256": hashlib.sha256(task_data).hexdigest(),
            },
            {
                "path": "parent-build.md",
                "role": "parent_build",
                "bytes": len(parent_data),
                "sha256": hashlib.sha256(parent_data).hexdigest(),
            },
            {
                "path": "decoded_parent.md",
                "role": "parent_decode",
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        ],
    }
    manifest["pack_sha256"] = worker._pack_sha(manifest)
    out = root / "MANIFEST.json"
    out.write_text(json.dumps(manifest), encoding="utf-8")
    return out


def test_load_pack_refuses_hash_drift(monkeypatch):
    root = _case("hash-drift")
    monkeypatch.setattr(worker, "REPO_ROOT", root)
    manifest = _manifest(root)
    (root / "decoded_parent.md").write_text("changed", encoding="utf-8")
    with pytest.raises(RuntimeError, match="changed after the pack was built"):
        worker.load_pack(manifest)


def test_missing_headings_is_exact_and_case_insensitive():
    text = "### classification\nvalue\n\n### Selected direction\nvalue\n"
    assert worker.missing_headings(
        text,
        ["### Classification", "### Selected direction", "### Challenge test"],
    ) == ["### Challenge test"]


@pytest.mark.parametrize("marker", worker.DEFERRED_ANSWER_MARKERS)
def test_every_deep_think_holding_marker_is_not_a_final_answer(marker):
    assert worker.is_deferred_answer(f"Status: {marker}. Please wait.")
    assert not worker.is_deferred_answer("### Classification\nMETHOD: STEP-UP-SAME-SOURCE")


def test_deferred_phrase_inside_structured_final_answer_is_not_a_hold_notice():
    answer = (
        "### Classification\nThe render may take some time in production.\n\n"
        "### Selected direction\nUse the current parent."
    )
    assert not worker.is_deferred_answer(answer)


def test_wait_never_accepts_constant_partial_without_run_or_terminal_proof(monkeypatch):
    clock = [0.0]

    class Adapter:
        name = "gemini"

        def answer_count(self, page):
            return 1

        def answer_text(self, page):
            return "partial answer " * 20

        def is_running(self, page):
            return False

        def completion_state(self, page):
            return None

    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    with pytest.raises(RuntimeError, match="did not finish"):
        worker.wait_for_answer(object(), Adapter(), before_count=0, timeout_s=10)


def test_wait_accepts_stable_answer_only_after_observing_run_end(monkeypatch):
    clock = [0.0]

    class Adapter:
        name = "gemini"

        def __init__(self):
            self.poll = 0

        def answer_count(self, page):
            return 1

        def answer_text(self, page):
            return "finished answer " * 20

        def is_running(self, page):
            self.poll += 1
            return self.poll == 1

        def completion_state(self, page):
            return None

    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    assert worker.wait_for_answer(
        object(), Adapter(), before_count=0, timeout_s=20
    ).startswith("finished answer")


def test_deferred_holding_text_cannot_hide_observed_run_state(monkeypatch):
    clock = [0.0]

    class Adapter:
        name = "gemini"

        def __init__(self):
            self.poll = 0

        def answer_count(self, page):
            return 1

        def answer_text(self, page):
            if self.poll == 0:
                return "I am working on it. Please wait."
            return "finished answer " * 20

        def is_running(self, page):
            self.poll += 1
            return self.poll == 1

        def completion_state(self, page):
            return None

    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    assert worker.wait_for_answer(
        object(), Adapter(), before_count=0, timeout_s=20
    ).startswith("finished answer")


def test_full_prompt_proof_refuses_a_same_prefix_truncation():
    prompt = "Read everything.\n" + ("evidence " * 100)
    worker.prove_full_prompt(prompt, prompt, "test")
    with pytest.raises(RuntimeError, match="exact full prompt"):
        worker.prove_full_prompt(prompt[:-20], prompt, "test")


def test_attachment_name_check_requires_every_file():
    paths = ["C:/repo/a.md", "C:/repo/b.md"]
    assert worker.missing_attachment_names("a.md is ready", paths) == ["b.md"]
    assert worker.missing_attachment_names("A.MD and B.MD", paths) == []


def test_best_chatgpt_setting_uses_strongest_visible_level():
    options = ["Instant", "Medium", "High", "Extra High"]
    assert worker.choose_visible_option(
        options, worker.CHATGPT_REASONING_LEVELS["best"]
    ) == "Extra High"


def test_best_gemini_setting_prefers_deep_think_then_extended():
    assert worker.choose_visible_option(
        ["Standard", "Extended"], worker.GEMINI_THINKING_LEVELS["best"]
    ) == "Extended"
    assert worker.choose_visible_option(
        ["Standard", "Deep Think"], worker.GEMINI_THINKING_LEVELS["best"]
    ) == "Deep Think"


def test_run_turn_verifies_all_attachments_before_send(monkeypatch):
    events = []

    class FakeAdapter:
        name = "fake"

        def new_chat(self, page):
            events.append("new_chat")

        def select_settings(self, page, model, quality, thinking):
            events.append("model")
            return {"model": "resolved", "thinking": "high"}

        def attach(self, page, paths):
            events.append("attach")

        def verify_attachments(self, page, paths):
            events.append("verify")

        def answer_count(self, page):
            events.append("count")
            return 0

        def send(self, page, prompt):
            events.append("send")

        def extract(self, page):
            events.append("extract")
            return (
                "### Classification\nanswer\n" + worker._nonce_line(self.turn_nonce),
                "fake",
            )

        def conversation_url(self, page):
            return "https://example.test/c/1"

    monkeypatch.setattr(
        worker,
        "wait_for_answer",
        lambda page, adapter, before_count, timeout_s: events.append("wait"),
    )
    result = worker.run_turn(
        object(), FakeAdapter(), ["a.md"], "prompt", "model", "best", None, 5
    )

    assert events.index("verify") < events.index("send")
    assert events == [
        "new_chat",
        "model",
        "attach",
        "verify",
        "count",
        "send",
        "wait",
        "extract",
    ]
    assert result[1] == "fake"
    assert result[2]["model"] == "resolved"
    assert result[2]["thinking"] == "high"
    assert result[2]["turn_nonce"]
    assert result[3] == "https://example.test/c/1"


def test_pending_checkpoint_can_restore_resume_settings():
    root = _case("pending-checkpoint")
    manifest_path = _manifest(root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    output = root / "answer.md"
    settings = {"model": "current ChatGPT family", "thinking": "High"}
    url = "https://chatgpt.com/c/test-conversation"

    worker.write_pending(
        output,
        manifest=manifest,
        provider="chatgpt",
        resolved_settings=settings,
        conversation_url=url,
        email="owner@example.com",
        before_answer_count=2,
    )

    assert worker.existing_result_settings(
        output,
        url,
        manifest=manifest,
        provider="chatgpt",
        email="owner@example.com",
    ) == (settings, 2)
    pending = json.loads(
        output.with_suffix(".md.pending.json").read_text(encoding="utf-8")
    )
    assert pending["resumable"] is True
    assert pending["pack_sha256"] == manifest["pack_sha256"]
    assert pending["email"] == "owner@example.com"
    assert pending["before_answer_count"] == 2
    assert [item["path"] for item in pending["attachments"]] == [
        "task.md",
        "parent-build.md",
        "decoded_parent.md",
    ]


def test_resume_requires_checkpoint_pack_provider_and_account_identity():
    root = _case("resume-identity")
    manifest = json.loads(_manifest(root).read_text(encoding="utf-8"))
    output = root / "answer.md"
    url = "https://chatgpt.com/c/resume-id"
    worker.write_pending(
        output,
        manifest=manifest,
        provider="chatgpt",
        resolved_settings={"model": "GPT", "thinking": "High"},
        conversation_url=url,
        email="owner@example.com",
        before_answer_count=0,
    )

    with pytest.raises(RuntimeError, match="provider"):
        worker.existing_result_settings(
            output,
            url,
            manifest=manifest,
            provider="gemini",
            email="owner@example.com",
        )
    with pytest.raises(RuntimeError, match="different account"):
        worker.existing_result_settings(
            output,
            url,
            manifest=manifest,
            provider="chatgpt",
            email="other@example.com",
        )
    changed = dict(manifest, pack_sha256="other-pack")
    with pytest.raises(RuntimeError, match="different creative pack"):
        worker.existing_result_settings(
            output,
            url,
            manifest=changed,
            provider="chatgpt",
            email="owner@example.com",
        )
    pending_path = output.with_suffix(".md.pending.json")
    pending = json.loads(pending_path.read_text(encoding="utf-8"))
    pending["attachments"] = pending["attachments"][:-1]
    pending_path.write_text(json.dumps(pending), encoding="utf-8")
    with pytest.raises(RuntimeError, match="attachment list"):
        worker.existing_result_settings(
            output,
            url,
            manifest=manifest,
            provider="chatgpt",
            email="owner@example.com",
        )


def test_resume_refuses_missing_checkpoint_and_wrong_provider_url():
    root = _case("resume-missing")
    with pytest.raises(RuntimeError, match="requires the matching pending"):
        worker.existing_result_settings(
            root / "none.md",
            "https://chatgpt.com/c/id",
            manifest={"pack_sha256": "pack"},
            provider="chatgpt",
            email="owner@example.com",
        )
    with pytest.raises(RuntimeError, match="invalid chatgpt conversation URL"):
        worker._conversation_identity("https://gemini.google.com/app/id", "chatgpt")


def test_exact_account_proof_fails_closed(monkeypatch):
    monkeypatch.setattr(worker.chat_worker, "_logged_in_email", lambda page: None)
    with pytest.raises(RuntimeError, match="did not expose an account email"):
        worker.prove_chatgpt_account(object(), "owner@example.com")

    monkeypatch.setattr(
        worker, "_visible_email_addresses", lambda page: ["other@example.com"]
    )

    class EmptyAccount:
        def count(self):
            return 0

    class Locator:
        @property
        def first(self):
            return EmptyAccount()

    page = SimpleNamespace(locator=lambda selector: Locator())
    with pytest.raises(RuntimeError, match="different account"):
        worker.prove_gemini_account(page, "owner@example.com")


def test_gemini_model_proof_refuses_missing_or_wrong_visible_label(monkeypatch):
    monkeypatch.setattr(worker.gemini_worker, "select_model", lambda page, model: None)
    with pytest.raises(RuntimeError, match="refusing to guess"):
        worker.GeminiAdapter().select_settings(object(), "3.1 Pro", "best", None)

    monkeypatch.setattr(
        worker.gemini_worker, "select_model", lambda page, model: "Flash"
    )
    with pytest.raises(RuntimeError, match="model proof failed"):
        worker.GeminiAdapter().select_settings(object(), "3.1 Pro", "best", None)


def test_gemini_attachment_proof_requires_visible_chip_names(monkeypatch):
    adapter = worker.GeminiAdapter()
    adapter._selected_names = ["a.md", "b.md"]
    called = []
    monkeypatch.setattr(
        worker,
        "wait_for_gemini_attachment_chips",
        lambda page, paths: called.append(list(paths)),
    )
    adapter.verify_attachments(object(), ["C:/a.md", "C:/b.md"])
    assert called == [["C:/a.md", "C:/b.md"]]


def test_gemini_attachment_proof_does_not_accept_zero_chips(monkeypatch):
    clock = [0.0]

    class NoBars:
        def count(self):
            return 0

    page = SimpleNamespace(locator=lambda selector: NoBars())
    monkeypatch.setattr(worker, "gemini_chip_snapshot", lambda page: "")
    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    with pytest.raises(RuntimeError, match="attachment-chip proof failed"):
        worker.wait_for_gemini_attachment_chips(page, ["C:/a.md"], timeout_s=1)


def test_private_run_profiles_are_unique_cloned_and_cleaned(monkeypatch):
    root = _case("private-run-profile")
    base = root / "static"
    base.mkdir(exist_ok=True)
    source = root / "source"
    source.mkdir(exist_ok=True)
    (source / "Cookies").write_text("session", encoding="utf-8")
    (source / "SingletonLock").write_text("locked", encoding="utf-8")
    monkeypatch.setattr(worker, "BASE_DIR", base)

    first = worker._new_run_profile("chatgpt", "owner@example.com", "firefox")
    second = worker._new_run_profile("chatgpt", "owner@example.com", "firefox")
    worker._copy_profile(source, first)

    assert first != second
    assert (first / "Cookies").read_text(encoding="utf-8") == "session"
    assert not (first / "SingletonLock").exists()
    worker._cleanup_run_profile(first)
    worker._cleanup_run_profile(second)
    assert not first.exists()
    assert not second.exists()


def test_parallel_chatgpt_configuration_never_reuses_the_profile(monkeypatch):
    root = _case("parallel-chatgpt-profiles")
    base = root / "static"
    base.mkdir(exist_ok=True)
    golden = root / "golden"
    golden.mkdir(exist_ok=True)
    (golden / "Cookies").write_text("session", encoding="utf-8")
    monkeypatch.setattr(worker, "BASE_DIR", base)
    monkeypatch.setattr(worker.chat_backend, "set_browser_mode", lambda mode: None)
    args = SimpleNamespace(
        firefox=True,
        headless=True,
        profile_dir=str(golden),
        email="owner@example.com",
        no_reseed=True,
    )

    first = worker.configure_chatgpt(args)
    first_bridge = Path(worker.chat_backend.CHROME_GOLDEN_DIR)
    second = worker.configure_chatgpt(args)
    second_bridge = Path(worker.chat_backend.CHROME_GOLDEN_DIR)

    assert first != second
    assert first_bridge.parent == first
    assert second_bridge.parent == second
    assert (first / "Cookies").is_file()
    assert (second / "Cookies").is_file()
    worker._cleanup_run_profile(first)
    worker._cleanup_run_profile(second)


def test_chrome_headless_setting_reaches_the_real_backend_launch(monkeypatch):
    captured = {}
    root = _local_case("chrome-headless-launch")

    class Context:
        pages = []

        def new_page(self):
            return object()

    monkeypatch.setattr(worker.chat_backend, "FIREFOX_MODE", False)
    monkeypatch.setattr(worker.chat_backend, "USER_DATA_DIR", None)
    monkeypatch.setattr(worker.chat_backend, "PROFILE_DIR", str(root / "profile"))
    monkeypatch.setattr(worker.chat_backend, "CHROME_HEADLESS", True)
    monkeypatch.setattr(worker.chat_backend, "inject_cookies", lambda context: None)
    monkeypatch.setattr(
        worker.chat_backend._bd,
        "launch_context",
        lambda playwright, mode, **kwargs: captured.update(kwargs) or Context(),
    )

    worker.chat_backend.launch(object())

    assert captured["headless"] is True


def test_second_firefox_launch_reports_reseed_or_bridge_repair(monkeypatch):
    root = _local_case(f"firefox-repair-launch-{time.time_ns()}")

    class Context:
        pages = []

        def new_page(self):
            return object()

    monkeypatch.setattr(worker.chat_backend, "FIREFOX_MODE", True)
    monkeypatch.setattr(worker.chat_backend, "USER_DATA_DIR", None)
    monkeypatch.setattr(worker.chat_backend, "PROFILE_DIR", str(root / "profile"))
    monkeypatch.setattr(worker.chat_backend, "REPAIR_DETECTION_ENABLED", True)
    monkeypatch.setattr(worker.chat_backend, "LOGIN_REPAIR_OCCURRED", False)
    monkeypatch.setattr(worker.chat_backend, "SESSION_LAUNCH_COUNT", 0)
    monkeypatch.setattr(worker.chat_backend, "inject_cookies", lambda context: None)
    monkeypatch.setenv("FIREFOX_HEADLESS", "1")
    monkeypatch.setattr(
        worker.chat_backend._bd,
        "launch_context",
        lambda playwright, mode, **kwargs: Context(),
    )

    worker.chat_backend.launch(object())
    assert worker.chat_backend.LOGIN_REPAIR_OCCURRED is False
    worker.chat_backend.launch(object())
    assert worker.chat_backend.LOGIN_REPAIR_OCCURRED is True


def test_verified_repair_with_existing_expired_cookies_is_persisted(monkeypatch):
    root = _local_case(f"persist-expired-cookie-repair-{time.time_ns()}")
    run_profile = root / "run"
    durable = root / "durable"
    run_profile.mkdir(exist_ok=True)
    durable.mkdir(exist_ok=True)
    (run_profile / "cookies.sqlite").write_bytes(b"repaired session")
    (durable / "cookies.sqlite").write_bytes(b"expired session")
    args = SimpleNamespace(
        resume_url=None,
        resume_before_count=0,
        email="owner@example.com",
        model=None,
        quality="best",
        thinking=None,
        answer_timeout=5,
    )
    persisted = []
    cleaned = []

    def configure(current_args):
        current_args._chatgpt_durable_profile = durable
        current_args._chatgpt_repair_started_ns = time.time_ns()
        current_args._chatgpt_run_had_session_before_launch = True
        worker.chat_backend.LOGIN_REPAIR_OCCURRED = False
        return run_profile

    class PlaywrightManager:
        def __enter__(self):
            return object()

        def __exit__(self, *args):
            return False

    class Context:
        def close(self):
            pass

    class Adapter:
        def verify_account(self, page, email):
            worker.chat_backend.LOGIN_REPAIR_OCCURRED = True

    monkeypatch.setattr(worker, "configure_chatgpt", configure)
    monkeypatch.setattr(
        worker.chat_worker,
        "_import_playwright",
        lambda: (lambda: PlaywrightManager(), False),
    )
    monkeypatch.setattr(
        worker.chat_worker,
        "launch_logged_in",
        lambda playwright, email: (Context(), object()),
    )
    monkeypatch.setattr(worker, "ChatGPTAdapter", Adapter)
    monkeypatch.setattr(
        worker,
        "run_turn",
        lambda *args, **kwargs: ("answer", "dom", {}, "https://chatgpt.com/c/1"),
    )
    monkeypatch.setattr(
        worker,
        "_persist_chatgpt_profile",
        lambda run, saved, **kwargs: persisted.append((run, saved)) or True,
    )
    monkeypatch.setattr(
        worker, "_cleanup_run_profile", lambda path: cleaned.append(path)
    )

    worker.run_chatgpt(args, {"message": "prompt"}, [], {})

    assert persisted == [(run_profile, durable)]
    assert cleaned == [run_profile]


def test_chrome_same_launch_login_repair_with_stale_cookies_is_persisted(
    monkeypatch,
):
    root = _local_case(f"persist-chrome-same-launch-repair-{time.time_ns()}")
    run_profile = root / "run"
    durable = root / "durable"
    run_profile.mkdir(exist_ok=True)
    durable.mkdir(exist_ok=True)
    (run_profile / "Cookies").write_bytes(b"fresh login after operator repair")
    (durable / "Cookies").write_bytes(b"expired cookie store")
    args = SimpleNamespace(
        resume_url=None,
        resume_before_count=0,
        email="owner@example.com",
        model=None,
        quality="best",
        thinking=None,
        answer_timeout=5,
    )
    persisted = []

    def configure(current_args):
        current_args._chatgpt_durable_profile = durable
        current_args._chatgpt_repair_started_ns = time.time_ns()
        current_args._chatgpt_run_had_session_before_launch = True
        worker.chat_backend.LOGIN_REPAIR_OCCURRED = False
        return run_profile

    class Page:
        email = None

        def goto(self, *args, **kwargs):
            return None

    page = Page()

    class PlaywrightManager:
        def __enter__(self):
            return object()

        def __exit__(self, *args):
            return False

    class Context:
        def close(self):
            pass

    class Adapter:
        def verify_account(self, current_page, email):
            assert current_page.email == email

    def launch_logged_in(playwright, email):
        assert worker.chat_worker.ensure_logged_in(page, email)
        return Context(), page

    monkeypatch.setattr(worker, "configure_chatgpt", configure)
    monkeypatch.setattr(
        worker.chat_worker,
        "_import_playwright",
        lambda: (lambda: PlaywrightManager(), False),
    )
    checks = [0]

    def is_logged_in(current):
        checks[0] += 1
        if checks[0] >= 2:
            current.email = args.email
        return bool(current.email)

    monkeypatch.setattr(worker.chat_backend, "FIREFOX_MODE", False)
    monkeypatch.setattr(worker.chat_worker, "dismiss_cookie_banner", lambda page: None)
    monkeypatch.setattr(worker.chat_worker, "is_logged_in", is_logged_in)
    monkeypatch.setattr(worker.chat_worker, "_logged_in_email", lambda current: current.email)
    monkeypatch.setattr(worker.chat_worker, "launch_logged_in", launch_logged_in)
    monkeypatch.setattr(worker, "ChatGPTAdapter", Adapter)
    monkeypatch.setattr(
        worker,
        "run_turn",
        lambda *args, **kwargs: ("answer", "dom", {}, "https://chatgpt.com/c/1"),
    )
    monkeypatch.setattr(
        worker,
        "_persist_chatgpt_profile",
        lambda run, saved, **kwargs: persisted.append((run, saved)) or True,
    )
    monkeypatch.setattr(worker, "_cleanup_run_profile", lambda path: None)

    worker.run_chatgpt(args, {"message": "prompt"}, [], {})

    assert persisted == [(run_profile, durable)]
    assert worker.chat_backend.LOGIN_REPAIR_OCCURRED is True


def test_submit_probe_failure_is_only_diagnostic(monkeypatch):
    monkeypatch.setattr(
        worker,
        "_chatgpt_submit_probe",
        lambda page: (_ for _ in ()).throw(RuntimeError("page closed")),
    )
    assert worker._safe_chatgpt_submit_probe(object()) == {
        "diagnostic_error": "RuntimeError"
    }


def test_external_tool_inspection_failure_is_not_treated_as_tools_off():
    page = SimpleNamespace(
        evaluate=lambda script: (_ for _ in ()).throw(RuntimeError("DOM gone"))
    )
    with pytest.raises(RuntimeError, match="refusing to assume tools are off"):
        worker.active_external_tools(page)


def test_noop_copy_click_cannot_return_previous_job_clipboard(monkeypatch):
    clock = [0.0]

    class Page:
        clipboard = "previous job answer " * 20

        def evaluate(self, script, *args):
            if args:
                self.clipboard = args[0]
            return self.clipboard

    class Button:
        def click(self, **kwargs):
            pass

    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(
        worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    assert worker._click_copy_and_read(Page(), Button(), "job-a") is None


def test_copy_result_must_be_a_new_value_written_by_this_click(monkeypatch):
    page = SimpleNamespace(clipboard="previous")

    def evaluate(script, *args):
        if args:
            page.clipboard = args[0]
        return page.clipboard

    page.evaluate = evaluate

    class Button:
        def click(self, **kwargs):
            page.clipboard = (
                "current turn answer " * 20 + "\n" + worker._nonce_line("job-a")
            )

    assert worker._click_copy_and_read(page, Button(), "job-a").startswith(
        "current turn"
    )


def test_parallel_clipboard_answer_from_another_turn_is_rejected():
    shared = {"clipboard": "old"}

    class Page:
        def evaluate(self, script, *args):
            if args:
                shared["clipboard"] = args[0]
            return shared["clipboard"]

    class Button:
        def __init__(self, answer):
            self.answer = answer

        def click(self, **kwargs):
            shared["clipboard"] = self.answer

    answer_a = "job a answer " * 20 + "\n" + worker._nonce_line("job-a")
    wrong_for_b = "other job answer " * 20 + "\n" + worker._nonce_line("job-a")
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(
            worker._click_copy_and_read, Page(), Button(answer_a), "job-a"
        )
        future_b = pool.submit(
            worker._click_copy_and_read, Page(), Button(wrong_for_b), "job-b"
        )
    assert future_a.result() == answer_a
    assert future_b.result() is None


def test_verified_login_profile_is_copied_back_to_reusable_profile():
    root = _local_case(f"persist-repaired-login-{time.time_ns()}")
    run_profile = root / "run"
    durable = root / "durable"
    run_profile.mkdir(exist_ok=True)
    repair_started_ns = time.time_ns()
    (run_profile / "cookies.sqlite").write_bytes(b"verified session")

    worker._persist_chatgpt_profile(
        run_profile, durable, repair_started_ns=repair_started_ns
    )

    assert (durable / "cookies.sqlite").read_bytes() == b"verified session"


def test_profile_repair_race_keeps_the_freshest_login():
    root = _local_case(f"persist-repair-race-{time.time_ns()}")
    older = root / "older"
    newer = root / "newer"
    durable = root / "durable"
    older.mkdir(exist_ok=True)
    newer.mkdir(exist_ok=True)
    older_started = time.time_ns()
    (older / "cookies.sqlite").write_bytes(b"older login")
    newer_started = time.time_ns()
    (newer / "cookies.sqlite").write_bytes(b"newer login")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                worker._persist_chatgpt_profile,
                older,
                durable,
                repair_started_ns=older_started,
            ),
            pool.submit(
                worker._persist_chatgpt_profile,
                newer,
                durable,
                repair_started_ns=newer_started,
            ),
        ]
        [future.result() for future in futures]

    assert (durable / "cookies.sqlite").read_bytes() == b"newer login"
    assert worker._profile_repair_version(durable) == newer_started


def test_normal_authenticated_clone_is_not_a_login_repair():
    assert not worker._login_repair_occurred(
        authenticated=True,
        backend_reported_repair=False,
        had_session_before_launch=True,
        has_session_after_run=True,
    )


def test_run_turn_checkpoints_after_send_before_wait(monkeypatch):
    events = []

    class FakeAdapter:
        name = "fake"

        def new_chat(self, page):
            events.append("new_chat")

        def select_settings(self, page, model, quality, thinking):
            return {"model": "resolved", "thinking": "high"}

        def attach(self, page, paths):
            events.append("attach")

        def verify_attachments(self, page, paths):
            events.append("verify")

        def answer_count(self, page):
            return 0

        def send(self, page, prompt):
            events.append("send")

        def extract(self, page):
            return (
                "### Classification\nanswer\n" + worker._nonce_line(self.turn_nonce),
                "fake",
            )

        def conversation_url(self, page):
            return "https://example.test/c/1"

    monkeypatch.setattr(
        worker,
        "wait_for_answer",
        lambda page, adapter, before_count, timeout_s: events.append("wait"),
    )
    worker.run_turn(
        object(),
        FakeAdapter(),
        ["a.md"],
        "prompt",
        "model",
        "best",
        None,
        5,
        lambda settings, url, before_count: events.append(
            f"checkpoint:{before_count}"
        ),
    )

    assert events.index("send") < events.index("checkpoint:0") < events.index("wait")


def test_chatgpt_send_retries_with_enter_when_button_click_is_not_accepted(monkeypatch):
    """A browser click returning is not proof that ChatGPT accepted the turn."""
    clock = [0.0]

    class Composer:
        def __init__(self):
            self.text = ""

        def evaluate(self, script):
            return None

        def fill(self, value):
            self.text = value

        def inner_text(self):
            return self.text

    class Button:
        def __init__(self):
            self.clicks = 0

        def click(self, **kwargs):
            self.clicks += 1  # Browser action succeeds, app state does not move.

    class Locator:
        def __init__(self, item=None, count=0):
            self.item = item
            self._count = count

        def count(self):
            return self._count

        @property
        def last(self):
            return self.item

    class Keyboard:
        def __init__(self, page):
            self.page = page
            self.pressed = []

        def insert_text(self, value):
            self.page.composer.text = value

        def press(self, key):
            self.pressed.append(key)
            if key == "Enter":
                self.page.composer.text = ""
                self.page.url = "https://chatgpt.com/c/accepted"

    class Page:
        def __init__(self):
            self.url = "https://chatgpt.com/"
            self.composer = Composer()
            self.button = Button()
            self.keyboard = Keyboard(self)

        def locator(self, selector):
            if selector == worker.CHATGPT_USER:
                return Locator(count=0)
            return Locator(self.button, count=1)

        def screenshot(self, **kwargs):
            return None

    page = Page()
    monkeypatch.setattr(worker.chat_backend, "_click_composer", lambda page: page.composer)
    monkeypatch.setattr(worker, "_chatgpt_submit_probe", lambda page: {})
    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    worker.ChatGPTAdapter().send(page, "Read the attached files and return Markdown.")

    assert page.button.clicks == 1
    assert page.keyboard.pressed == ["Enter"]
    assert page.url == "https://chatgpt.com/c/accepted"


def test_chatgpt_send_does_not_press_enter_after_click_is_accepted(monkeypatch):
    """The fallback must not create a second turn after a real button submit."""
    clock = [0.0]

    class Composer:
        text = ""

        def evaluate(self, script):
            return None

        def fill(self, value):
            self.text = value

        def inner_text(self):
            return self.text

    class Keyboard:
        pressed = []

        def insert_text(self, value):
            page.composer.text = value

        def press(self, key):
            self.pressed.append(key)

    class Button:
        def click(self, **kwargs):
            page.composer.text = ""
            page.url = "https://chatgpt.com/c/accepted-by-click"

    class Locator:
        def __init__(self, item=None, count=0):
            self.item = item
            self._count = count

        def count(self):
            return self._count

        @property
        def last(self):
            return self.item

    class Page:
        url = "https://chatgpt.com/"
        composer = Composer()
        keyboard = Keyboard()

        def locator(self, selector):
            if selector == worker.CHATGPT_USER:
                return Locator(count=0)
            return Locator(Button(), count=1)

    page = Page()
    monkeypatch.setattr(worker.chat_backend, "_click_composer", lambda page: page.composer)
    monkeypatch.setattr(worker, "_chatgpt_submit_probe", lambda page: {})
    monkeypatch.setattr(worker.time, "time", lambda: clock[0])
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    worker.ChatGPTAdapter().send(page, "Read the attached files and return Markdown.")

    assert page.keyboard.pressed == []
    assert page.url == "https://chatgpt.com/c/accepted-by-click"
