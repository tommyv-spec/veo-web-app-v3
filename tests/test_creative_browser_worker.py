from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest


STATIC = Path(__file__).resolve().parents[1] / "static"
if str(STATIC) not in sys.path:
    sys.path.insert(0, str(STATIC))

import creative_browser_worker as worker


TEST_ROOT = Path(__file__).resolve().parents[2] / "output" / "creative_browser_worker_unit_tests"


def _case(name: str) -> Path:
    path = TEST_ROOT / name
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


def test_deep_think_holding_message_is_not_a_final_answer():
    assert worker.is_deferred_answer(
        "I'm on it. Responses with Deep Think can take some time, so check back in a bit."
    )
    assert not worker.is_deferred_answer("### Classification\nMETHOD: STEP-UP-SAME-SOURCE")


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
            return "### Classification\nanswer", "fake"

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
    assert result[1:] == (
        "fake",
        {"model": "resolved", "thinking": "high"},
        "https://example.test/c/1",
    )


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
    )

    assert worker.existing_result_settings(output, url) == settings
    pending = json.loads(
        output.with_suffix(".md.pending.json").read_text(encoding="utf-8")
    )
    assert pending["resumable"] is True
    assert pending["pack_sha256"] == manifest["pack_sha256"]
    assert [item["path"] for item in pending["attachments"]] == [
        "task.md",
        "parent-build.md",
        "decoded_parent.md",
    ]


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
            return "### Classification\nanswer", "fake"

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
        lambda settings, url: events.append("checkpoint"),
    )

    assert events.index("send") < events.index("checkpoint") < events.index("wait")


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
