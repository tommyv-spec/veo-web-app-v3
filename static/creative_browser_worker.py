#!/usr/bin/env python3
"""Run one frozen creative-reasoning pack through ChatGPT or Gemini web UI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import chatgpt_image_backend as chat_backend
import chatgpt_image_worker as chat_worker
import gemini_decode_worker as gemini_worker
import gemini_video_worker as gemini_video


MAX_ATTACHMENTS = 10
DEFAULT_TIMEOUT_S = int(os.environ.get("CREATIVE_BROWSER_ANSWER_TIMEOUT_S", "1800"))
CHATGPT_ASSISTANT = "[data-message-author-role='assistant']"
CHATGPT_USER = "[data-message-author-role='user']"
CHATGPT_MODEL_PICKERS = (
    "[data-testid='model-switcher-dropdown-button']",
    "[data-testid='model-switcher']",
    "[data-testid*='mode' i]",
    "[aria-label*='model selector' i]",
    "[aria-label*='mode picker' i]",
    "[aria-label*='model' i]",
)
MENU_OPTION_SELECTORS = (
    "[role=menuitem]",
    "[role=option]",
    "gem-menu-item",
)
CHATGPT_REASONING_LEVELS = {
    "best": ("Power", "Pro Extended", "Extra High", "High", "Medium"),
    "balanced": ("High", "Medium"),
    "fast": ("Instant", "Medium", "Low"),
}
CHATGPT_EFFORT_SLIDER = {
    "Low": 0,
    "Medium": 1,
    "High": 2,
    "Extra High": 3,
    "Power": 4,
}
GEMINI_THINKING_LEVELS = {
    "best": ("Deep Think", "Extended thinking", "Extended"),
    "balanced": ("Extended thinking", "Extended", "Standard"),
    "fast": ("Standard",),
}
EXTERNAL_TOOL_LABELS = (
    "search",
    "deep research",
    "research",
    "canvas",
    "study and learn",
    "study mode",
)
DEFERRED_ANSWER_MARKERS = (
    "i'm on it",
    "check back in a bit",
    "can take some time",
)


def log(message: str) -> None:
    print(f"[creative-browser] {message}", flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _repo_path(relative: str) -> Path:
    path = (REPO_ROOT / relative).resolve()
    try:
        path.relative_to(REPO_ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError(f"manifest path escapes the repo: {relative}") from exc
    return path


def _pack_sha(manifest: dict) -> str:
    payload = {
        "schema": manifest.get("schema"),
        "method": manifest.get("method"),
        "source_contract": manifest.get("source_contract"),
        "message": manifest.get("message"),
        "required_headings": manifest.get("required_headings"),
        "files": manifest.get("files"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(encoded)


def load_pack(pack: str | Path) -> tuple[dict, list[str]]:
    path = Path(pack)
    if path.is_dir():
        path = path / "MANIFEST.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read creative pack {path}: {exc}") from exc

    if manifest.get("schema") != 2:
        raise RuntimeError(f"unsupported creative pack schema: {manifest.get('schema')!r}")
    files = manifest.get("files") or []
    if not files or len(files) > MAX_ATTACHMENTS:
        raise RuntimeError(
            f"pack has {len(files)} attachments; expected 1-{MAX_ATTACHMENTS}"
        )
    if manifest.get("attachment_count") != len(files):
        raise RuntimeError("pack attachment_count does not match files")
    if not str(manifest.get("message") or "").strip():
        raise RuntimeError("pack send message is empty")
    if len(manifest.get("required_headings") or []) < 3:
        raise RuntimeError("pack has fewer than three required output headings")
    task_entries = [item for item in files if item.get("role") == "task"]
    if len(task_entries) != 1 or task_entries[0].get("path") != manifest.get("task"):
        raise RuntimeError("pack must contain exactly one matching task attachment")
    method = manifest.get("method")
    contract = manifest.get("source_contract") or {}
    if method not in {"step-up", "innovation"} or contract.get("method") != method:
        raise RuntimeError("pack has no valid step-up/innovation source contract")
    expected_roles = {
        "parent_build": contract.get("parent_build"),
        "parent_decode": contract.get("parent_decode"),
    }
    if method == "innovation":
        expected_roles["second_source_decode"] = contract.get(
            "second_source_decode"
        )
    elif contract.get("second_source_decode"):
        raise RuntimeError("step-up pack contains a forbidden second-source decode")
    for role, expected_path in expected_roles.items():
        entries = [item for item in files if item.get("role") == role]
        if len(entries) != 1 or entries[0].get("path") != expected_path:
            raise RuntimeError(
                f"pack must contain exactly one task-declared {role} attachment"
            )
    if _pack_sha(manifest) != manifest.get("pack_sha256"):
        raise RuntimeError("pack_sha256 does not match the manifest")

    resolved: list[str] = []
    seen: set[str] = set()
    for item in files:
        rel = str(item.get("path") or "")
        key = rel.casefold()
        if not rel or key in seen:
            raise RuntimeError(f"missing or duplicate manifest path: {rel!r}")
        seen.add(key)
        file_path = _repo_path(rel)
        if not file_path.is_file():
            raise RuntimeError(f"attachment is missing: {rel}")
        data = file_path.read_bytes()
        if len(data) != item.get("bytes") or sha256_bytes(data) != item.get("sha256"):
            raise RuntimeError(
                f"attachment changed after the pack was built: {rel}. Rebuild the pack."
            )
        resolved.append(str(file_path))
    return manifest, resolved


def missing_headings(text: str, headings: list[str]) -> list[str]:
    found = {line.strip().casefold() for line in text.splitlines() if line.startswith("### ")}
    return [heading for heading in headings if heading.strip().casefold() not in found]


def clean_answer(text: str) -> str:
    value = (text or "").replace("\r\n", "\n").strip()
    value = re.sub(r"^```(?:markdown|md)?\s*\n", "", value, flags=re.I)
    value = re.sub(r"\n```\s*$", "", value)
    return value.strip() + "\n"


def is_deferred_answer(text: str) -> bool:
    folded = (text or "").casefold()
    return bool(folded) and all(
        marker in folded for marker in ("i'm on it", "check back")
    ) or any(marker in folded for marker in DEFERRED_ANSWER_MARKERS[:1])


def attachment_snapshot(page) -> str:
    try:
        return page.evaluate(
            """() => {
                const bits = [document.body ? (document.body.innerText || '') : ''];
                for (const el of document.querySelectorAll('[aria-label], [title]')) {
                    const aria = el.getAttribute('aria-label');
                    const title = el.getAttribute('title');
                    if (aria) bits.push(aria);
                    if (title) bits.push(title);
                }
                return bits.join('\n');
            }"""
        ) or ""
    except Exception:
        try:
            return page.locator("body").inner_text() or ""
        except Exception:
            return ""


def missing_attachment_names(snapshot: str, paths: list[str]) -> list[str]:
    folded = (snapshot or "").casefold()
    return [Path(path).name for path in paths if Path(path).name.casefold() not in folded]


def wait_for_attachment_names(page, paths: list[str], timeout_s: int = 240) -> None:
    deadline = time.time() + timeout_s
    last_missing = [Path(path).name for path in paths]
    while time.time() < deadline:
        last_missing = missing_attachment_names(attachment_snapshot(page), paths)
        busy = False
        try:
            bars = page.locator("[role=progressbar]")
            busy = any(bars.nth(i).is_visible() for i in range(min(bars.count(), 20)))
        except Exception:
            pass
        if not last_missing and not busy:
            log(f"verified all {len(paths)} attachment names in the composer")
            return
        time.sleep(2)
    raise RuntimeError(
        "attachment verification failed; missing from the composer: "
        + ", ".join(last_missing)
    )


def _visible(page, selector: str) -> bool:
    try:
        items = page.locator(selector)
        return any(items.nth(i).is_visible() for i in range(min(items.count(), 10)))
    except Exception:
        return False


def _normalise_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def choose_visible_option(options: list[str], priorities: tuple[str, ...]) -> str | None:
    """Return the strongest requested option that is actually visible."""
    labelled = [(option, _normalise_label(option)) for option in options if option.strip()]
    for priority in priorities:
        wanted = _normalise_label(priority)
        exact = next((raw for raw, normal in labelled if normal == wanted), None)
        if exact:
            return exact
        partial = next((raw for raw, normal in labelled if wanted in normal), None)
        if partial:
            return partial
    return None


def _visible_menu_options(page) -> list[tuple[str, object]]:
    found: list[tuple[str, object]] = []
    seen: set[str] = set()
    for selector in MENU_OPTION_SELECTORS:
        try:
            items = page.locator(selector)
            for index in range(min(items.count(), 80)):
                item = items.nth(index)
                if not item.is_visible():
                    continue
                text = " ".join(
                    part.strip()
                    for part in (
                        item.inner_text() or "",
                        item.get_attribute("aria-label") or "",
                    )
                    if part and part.strip()
                )
                key = _normalise_label(text)
                if text and key not in seen:
                    seen.add(key)
                    found.append((text, item))
        except Exception:
            continue
    return found


def _click_priority_option(page, priorities: tuple[str, ...]) -> tuple[str | None, list[str]]:
    options = _visible_menu_options(page)
    labels = [label for label, _ in options]
    choice = choose_visible_option(labels, priorities)
    if not choice:
        return None, labels
    wanted = _normalise_label(choice)
    for label, item in options:
        if _normalise_label(label) == wanted:
            item.click(timeout=10000)
            time.sleep(1)
            return label, labels
    return None, labels


def _click_control(item) -> None:
    try:
        item.click(timeout=10000)
    except Exception:
        try:
            item.evaluate(
                """el => {
                    let node = el.parentElement;
                    for (let i = 0; node && i < 7; i++, node = node.parentElement) {
                        const clickable = node.matches(
                            'button, [role="button"], [aria-haspopup="menu"], [tabindex]'
                        ) || getComputedStyle(node).cursor === 'pointer';
                        if (clickable) {
                            node.click();
                            return;
                        }
                    }
                    el.click();
                }"""
            )
        except Exception:
            item.click(force=True, timeout=10000)
    time.sleep(1)


def active_external_tools(page) -> list[str]:
    """List evidence-changing composer tools that are visibly enabled."""
    try:
        values = page.evaluate(
            """() => Array.from(document.querySelectorAll(
                'button[aria-pressed="true"], [role="switch"][aria-checked="true"]'
            )).map(el => [
                el.innerText || '',
                el.getAttribute('aria-label') || '',
                el.getAttribute('title') || ''
            ].filter(Boolean).join(' '))"""
        ) or []
    except Exception:
        return []
    return [
        str(value).strip()
        for value in values
        if any(token in str(value).casefold() for token in EXTERNAL_TOOL_LABELS)
    ]


def disable_external_tools(page) -> list[str]:
    """Turn off web/research modes so both providers use only the frozen pack."""
    active = active_external_tools(page)
    for label in active:
        safe = label.replace("'", "\\'")
        selectors = (
            f"button[aria-pressed='true'][aria-label*='{safe}' i]",
            f"button[aria-pressed='true']:has-text('{safe}')",
        )
        for selector in selectors:
            try:
                button = page.locator(selector).first
                if button.count() and button.is_visible():
                    button.click(timeout=10000)
                    time.sleep(0.5)
                    break
            except Exception:
                continue
    remaining = active_external_tools(page)
    if remaining:
        raise RuntimeError(
            "external tools remain enabled; refusing an attachments-only benchmark: "
            + ", ".join(remaining)
        )
    return active


def _chatgpt_submit_probe(page) -> dict:
    """Return non-content DOM evidence for the current ChatGPT submit surface.

    This intentionally records no prompt or attachment contents. It exists to
    distinguish a real submit event from a no-op click on a stale/disabled node.
    """
    return page.evaluate(
        """() => {
            if (!window.__creativeSubmitProbeInstalled) {
                window.__creativeSubmitProbeInstalled = true;
                window.__creativeSubmitEvents = [];
                const record = (stage) => (event) => {
                    const target = event.target && event.target.closest
                        ? event.target.closest('button, form, #prompt-textarea')
                        : null;
                    window.__creativeSubmitEvents.push({
                        type: event.type,
                        stage,
                        defaultPrevented: !!event.defaultPrevented,
                        tag: target ? target.tagName : null,
                        testid: target ? target.getAttribute('data-testid') : null,
                        aria: target ? target.getAttribute('aria-label') : null,
                        buttonType: target ? target.getAttribute('type') : null
                    });
                };
                for (const type of ['pointerdown', 'mousedown', 'click', 'submit']) {
                    document.addEventListener(type, record('capture'), true);
                    document.addEventListener(type, record('bubble'), false);
                }
            }
            const describeForm = (form) => form ? {
                tag: form.tagName,
                id: form.id || null,
                testid: form.getAttribute('data-testid'),
                action: form.getAttribute('action'),
                method: form.getAttribute('method')
            } : null;
            const composer = document.querySelector('#prompt-textarea');
            const selector = [
                'button[data-testid="send-button"]',
                'button[aria-label="Send prompt"]',
                'button[data-testid="composer-send-button"]'
            ].join(', ');
            const buttons = Array.from(document.querySelectorAll(selector));
            return {
                path: location.pathname,
                active: document.activeElement ? {
                    tag: document.activeElement.tagName,
                    id: document.activeElement.id || null,
                    testid: document.activeElement.getAttribute('data-testid')
                } : null,
                composer: composer ? {
                    tag: composer.tagName,
                    connected: composer.isConnected,
                    contenteditable: composer.getAttribute('contenteditable'),
                    role: composer.getAttribute('role'),
                    textLength: (composer.innerText || '').length,
                    form: describeForm(composer.closest('form'))
                } : null,
                buttons: buttons.map((button, index) => {
                    const rect = button.getBoundingClientRect();
                    const style = getComputedStyle(button);
                    return {
                        index,
                        connected: button.isConnected,
                        testid: button.getAttribute('data-testid'),
                        aria: button.getAttribute('aria-label'),
                        type: button.getAttribute('type'),
                        disabled: !!button.disabled,
                        ariaDisabled: button.getAttribute('aria-disabled'),
                        rect: {
                            x: Math.round(rect.x), y: Math.round(rect.y),
                            width: Math.round(rect.width), height: Math.round(rect.height)
                        },
                        display: style.display,
                        visibility: style.visibility,
                        pointerEvents: style.pointerEvents,
                        form: describeForm(button.closest('form')),
                        insideComposerParent: !!(composer && composer.parentElement &&
                            composer.parentElement.contains(button))
                    };
                }),
                events: (window.__creativeSubmitEvents || []).slice(-20)
            };
        }"""
    )


def wait_for_answer(page, adapter, before_count: int, timeout_s: int) -> str:
    deadline = time.time() + timeout_s
    last = ""
    stable = 0
    deferred_logged = False
    while time.time() < deadline:
        count = adapter.answer_count(page)
        text = adapter.answer_text(page) if count > before_count else ""
        stable = stable + 1 if text and text == last else 0
        last = text
        if text and is_deferred_answer(text):
            stable = 0
            if not deferred_logged:
                log(f"{adapter.name} returned a deferred-work notice; waiting for final answer")
                deferred_logged = True
            time.sleep(5)
            continue
        completion = adapter.completion_state(page)
        if len(text.strip()) >= 100:
            if completion is True:
                log(f"{adapter.name} answer complete ({len(text)} visible chars)")
                return text
            if completion is None and not adapter.is_running(page) and stable >= 3:
                log(f"{adapter.name} answer stable ({len(text)} visible chars)")
                return text
        time.sleep(2)
    raise RuntimeError(f"{adapter.name} did not finish an answer within {timeout_s}s")


class GeminiAdapter:
    name = "gemini"

    def __init__(self) -> None:
        self._selected_names: list[str] = []

    def new_chat(self, page) -> None:
        gemini_worker.new_chat(page)

    def select_settings(
        self,
        page,
        model: str | None,
        quality: str,
        thinking: str | None,
    ) -> dict:
        resolved_model = gemini_worker.select_model(page, model or "Pro") or model or "Pro"
        if "flash" in resolved_model.casefold() and quality != "fast":
            raise RuntimeError(
                "Gemini resolved to Flash for a reasoning benchmark; choose Pro or use --quality fast"
            )

        priorities = (thinking,) if thinking else GEMINI_THINKING_LEVELS[quality]
        picker = gemini_worker.find(page, "model_picker", required=False)
        available: list[str] = []
        resolved_thinking = None
        if picker:
            _click_control(picker)
            resolved_thinking, available = _click_priority_option(page, priorities)
            if not resolved_thinking:
                nested = choose_visible_option(available, ("Thinking level", "Thinking"))
                if nested:
                    _click_priority_option(page, (nested,))
                    resolved_thinking, nested_options = _click_priority_option(
                        page, priorities
                    )
                    available.extend(
                        item for item in nested_options if item not in available
                    )
        if thinking and not resolved_thinking:
            raise RuntimeError(
                f"Gemini thinking level {thinking!r} is not available; visible options: {available}"
            )
        if not resolved_thinking:
            resolved_thinking = "account default (thinking control not exposed)"
            log(
                "Gemini did not expose a thinking-level control; recording the account default"
            )
        disabled = disable_external_tools(page)
        return {
            "quality_policy": quality,
            "requested_model": model,
            "model": resolved_model,
            "requested_thinking": thinking,
            "thinking": resolved_thinking,
            "external_tools": "off",
            "disabled_external_tools": disabled,
            "visible_picker_options": available,
        }

    def attach(self, page, paths: list[str]) -> None:
        menu = gemini_worker.find(page, "upload_menu", timeout_ms=10000)
        gemini_worker._open_upload_menu(page, menu)
        time.sleep(2)
        file_input = gemini_worker.find(
            page, "file_input", timeout_ms=10000, required=False
        )
        if file_input is None:
            file_input = page.locator(gemini_worker.SELECTORS["file_input"][0]).first
        if not file_input.count():
            raise RuntimeError("Gemini file input did not mount after opening upload")
        handle = file_input.element_handle(timeout=10000)
        if handle is None:
            raise RuntimeError("Gemini file input disappeared before file selection")
        handle.set_input_files(paths)
        self._selected_names = handle.evaluate(
            "el => Array.from(el.files || []).map(file => file.name)"
        )
        expected = [Path(path).name for path in paths]
        if self._selected_names != expected:
            raise RuntimeError(
                "Gemini selected the wrong attachment list: "
                f"expected {expected}, got {self._selected_names}"
            )
        page.keyboard.press("Escape")
        gemini_worker.wait_for_uploads(page, len(paths))

    def verify_attachments(self, page, paths: list[str]) -> None:
        expected = [Path(path).name for path in paths]
        if self._selected_names != expected:
            raise RuntimeError("Gemini attachment selection proof was lost before Send")
        log(
            f"verified all {len(paths)} Gemini file names in the browser file input "
            "and waited for settled attachment chips"
        )

    def send(self, page, prompt: str) -> None:
        gemini_worker.send(page, prompt)

    def answer_count(self, page) -> int:
        for selector in ("model-response", "message-content"):
            try:
                count = page.locator(selector).count()
                if count:
                    return count
            except Exception:
                pass
        return 0

    def answer_text(self, page) -> str:
        for selector in ("model-response", "message-content"):
            try:
                items = page.locator(selector)
                if items.count():
                    return items.last.inner_text() or ""
            except Exception:
                pass
        return ""

    def is_running(self, page) -> bool:
        return _visible(page, ",".join(gemini_worker.SELECTORS["stop"]))

    def completion_state(self, page) -> bool | None:
        return None

    def extract(self, page) -> tuple[str, str]:
        for name, getter in (
            ("clipboard", lambda: gemini_worker._from_clipboard(page)),
            ("html", lambda: gemini_worker._from_html(page)),
            ("dom", lambda: gemini_worker._from_dom(page)),
        ):
            try:
                text = getter()
            except Exception as exc:
                log(f"Gemini {name} extraction failed: {type(exc).__name__}")
                continue
            if text and len(text.strip()) >= 100:
                return text, name
        raise RuntimeError("Gemini answer finished but no extraction path returned text")

    def conversation_url(self, page) -> str:
        return page.url or ""


class ChatGPTAdapter:
    name = "chatgpt"

    def new_chat(self, page) -> None:
        home = chat_backend.CHATGPT_URL
        page.goto(home, wait_until="domcontentloaded", timeout=45000)
        chat_backend.dismiss_cookie_banner(page)
        chat_backend._dismiss_modal(page)
        if not chat_backend.is_logged_in(page):
            raise RuntimeError("ChatGPT session is not logged in")
        page.locator(chat_backend.SEL["composer"]).first.wait_for(
            state="visible", timeout=30000
        )
        composer = page.locator(chat_backend.SEL["composer"]).first
        if (composer.inner_text() or "").strip():
            composer.click(force=True, timeout=10000)
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            time.sleep(0.5)
            if (composer.inner_text() or "").strip():
                raise RuntimeError("ChatGPT fresh chat retained a stale composer draft")
            log("cleared stale ChatGPT composer draft before settings and upload")

    def _picker(self, page):
        picker = None
        for selector in CHATGPT_MODEL_PICKERS:
            try:
                item = page.locator(selector).first
                if item.count() and item.is_visible():
                    picker = item
                    break
            except Exception:
                pass
        if picker is None:
            try:
                compact = page.get_by_text(
                    re.compile(
                        r"^\s*(?:GPT[- ]?)?\d[\w. -]*\s+(?:Pro|Thinking|Instant)\s*$",
                        re.I,
                    )
                ).first
                if compact.count() and compact.is_visible():
                    picker = compact
            except Exception:
                pass
        if picker is None:
            try:
                handle = page.evaluate_handle(
                    """(labels) => {
                        const wanted = new Set(labels.map(x => x.toLowerCase()));
                        const nodes = Array.from(document.querySelectorAll('*'));
                        for (const node of nodes) {
                            if (node.children.length) continue;
                            const text = (node.textContent || '').trim().toLowerCase();
                            if (!wanted.has(text)) continue;
                            const rect = node.getBoundingClientRect();
                            if (!rect.width || !rect.height) continue;
                            if (rect.x < window.innerWidth * 0.35 ||
                                rect.x > window.innerWidth * 0.85 ||
                                rect.y < window.innerHeight * 0.25 ||
                                rect.y > window.innerHeight * 0.75) continue;
                            return node.closest('button, [role="button"]') || node;
                        }
                        return null;
                    }""",
                    [
                        "Power",
                        "Pro Extended",
                        "Extra High",
                        "High",
                        "Medium",
                        "Instant",
                        "6 Pro",
                        "Pro",
                    ],
                )
                compact = handle.as_element()
                if compact and compact.is_visible():
                    picker = compact
            except Exception:
                pass
        if picker is None:
            for exact_label in (
                "Power",
                "Pro Extended",
                "Extra High",
                "High",
                "Medium",
                "Instant",
                "6 Pro",
                "Pro",
            ):
                try:
                    candidates = page.get_by_text(exact_label, exact=True)
                    viewport = page.evaluate(
                        "() => ({width: window.innerWidth, height: window.innerHeight})"
                    )
                    for index in range(candidates.count()):
                        compact = candidates.nth(index)
                        if not compact.is_visible():
                            continue
                        box = compact.bounding_box()
                        if (
                            box
                            and box.get("x", 0) > viewport["width"] * 0.35
                            and viewport["height"] * 0.25
                            < box.get("y", 0)
                            < viewport["height"] * 0.75
                        ):
                            picker = compact
                            break
                    if picker is not None:
                        break
                except Exception:
                    continue
        if picker is None:
            try:
                buttons = page.locator("button, [role='button'], [aria-haspopup='menu']")
                candidates = []
                for index in range(min(buttons.count(), 120)):
                    item = buttons.nth(index)
                    if not item.is_visible():
                        continue
                    label = " | ".join(
                        part.strip()
                        for part in (
                            item.inner_text() or "",
                            item.get_attribute("aria-label") or "",
                            item.get_attribute("data-testid") or "",
                            item.get_attribute("title") or "",
                        )
                        if part and part.strip()
                    )
                    if any(
                        token in label.casefold()
                        for token in ("model", "mode", "gpt", "thinking", "instant", "pro")
                    ):
                        candidates.append(label)
                log("ChatGPT picker diagnostics: " + repr(candidates[:30]))
            except Exception:
                pass
        return picker

    @staticmethod
    def _picker_label(picker) -> str:
        return " ".join(
            part
            for part in (
                picker.inner_text() or "",
                picker.get_attribute("aria-label") or "",
            )
            if part
        ).strip()

    def select_settings(
        self,
        page,
        model: str | None,
        quality: str,
        thinking: str | None,
    ) -> dict:
        priorities = (thinking,) if thinking else CHATGPT_REASONING_LEVELS[quality]
        picker = self._picker(page)
        if picker is None:
            available: list[str] = []
            if not model:
                try:
                    viewport = page.evaluate(
                        "() => ({width: window.innerWidth, height: window.innerHeight})"
                    )
                    page.mouse.click(
                        viewport["width"] * 0.72,
                        viewport["height"] * 0.45,
                    )
                    time.sleep(1)
                    open_debug_path = (
                        REPO_ROOT
                        / "output"
                        / "creative_browser_debug"
                        / "chatgpt-picker-open.png"
                    )
                    open_debug_path.parent.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(open_debug_path), full_page=False)
                    log(
                        "ChatGPT coordinate picker attempt at "
                        f"({viewport['width'] * 0.72:.0f}, {viewport['height'] * 0.45:.0f}); "
                        f"screenshot: {open_debug_path}"
                    )
                    slider_details = page.locator(
                        "[role='slider'], input[type='range']"
                    ).evaluate_all(
                        """els => els.map(el => ({
                            tag: el.tagName,
                            min: el.getAttribute('min'),
                            max: el.getAttribute('max'),
                            value: el.value || el.getAttribute('aria-valuenow'),
                            valueText: el.getAttribute('aria-valuetext'),
                            label: el.getAttribute('aria-label')
                        }))"""
                    )
                    log("ChatGPT effort slider diagnostics: " + repr(slider_details))
                    sliders = page.locator("[role='slider'], input[type='range']")
                    slider_choice = next(
                        (
                            item
                            for item in priorities
                            if item in CHATGPT_EFFORT_SLIDER
                        ),
                        None,
                    )
                    if sliders.count() and slider_choice:
                        slider = sliders.first
                        current_value = int(
                            slider.get_attribute("aria-valuenow")
                            or slider.get_attribute("value")
                        )
                        target_value = CHATGPT_EFFORT_SLIDER[slider_choice]
                        key = "ArrowRight" if target_value > current_value else "ArrowLeft"
                        for _ in range(abs(target_value - current_value)):
                            slider.press(key)
                            time.sleep(0.3)
                        verified_value = int(
                            slider.get_attribute("aria-valuenow")
                            or slider.get_attribute("value")
                        )
                        if verified_value != target_value:
                            raise RuntimeError(
                                "ChatGPT effort slider did not reach the requested value: "
                                f"wanted {target_value}, got {verified_value}"
                            )
                        page.keyboard.press("Escape")
                        disabled = disable_external_tools(page)
                        return {
                            "quality_policy": quality,
                            "requested_model": model,
                            "model": "current ChatGPT family; thinking-effort slider",
                            "requested_thinking": thinking,
                            "thinking": slider_choice,
                            "external_tools": "off",
                            "disabled_external_tools": disabled,
                            "visible_picker_options": list(CHATGPT_EFFORT_SLIDER),
                            "picker_path": "thinking-effort slider + aria-valuenow proof",
                            "slider_value": verified_value,
                        }
                    selected, available = _click_priority_option(page, priorities)
                    if selected:
                        disabled = disable_external_tools(page)
                        return {
                            "quality_policy": quality,
                            "requested_model": model,
                            "model": "current ChatGPT family; compact composer control",
                            "requested_thinking": thinking,
                            "thinking": selected,
                            "external_tools": "off",
                            "disabled_external_tools": disabled,
                            "visible_picker_options": available,
                            "picker_path": "fixed composer coordinate + exact option",
                        }
                    page.keyboard.press("Escape")
                except Exception:
                    pass
            try:
                debug_path = REPO_ROOT / "output" / "creative_browser_debug" / "chatgpt-picker.png"
                debug_path.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(debug_path), full_page=False)
                log(f"ChatGPT picker screenshot: {debug_path}")
            except Exception:
                pass
            raise RuntimeError(
                "ChatGPT model/reasoning picker is not exposed; cannot prove the requested quality"
            )

        current = self._picker_label(picker)
        available: list[str] = []
        if current.strip() == "Pro":
            resolved_model = "6 Pro (compact composer label)"
        elif choose_visible_option([current], tuple(sum(CHATGPT_REASONING_LEVELS.values(), ()) )):
            resolved_model = f"current ChatGPT family; composer level {current}"
        else:
            resolved_model = current or "not shown"
        if model and _normalise_label(model) not in _normalise_label(current):
            _click_control(picker)
            selected_model, available = _click_priority_option(page, (model,))
            if not selected_model:
                chat_backend._dismiss_modal(page)
                raise RuntimeError(
                    f"ChatGPT model {model!r} is not available; visible options: {available}"
                )
            resolved_model = selected_model
            picker = self._picker(page)
            current = self._picker_label(picker) if picker else selected_model

        current_thinking = choose_visible_option([current], priorities)
        resolved_thinking = (
            current_thinking
            if thinking and current_thinking
            else current_thinking
            if current_thinking == priorities[0]
            else None
        )
        if not resolved_thinking:
            for attempt in range(3):
                picker = self._picker(page)
                if not picker:
                    break
                _click_control(picker)
                resolved_thinking, level_options = _click_priority_option(
                    page, priorities
                )
                available.extend(item for item in level_options if item not in available)
                if resolved_thinking:
                    break
                page.keyboard.press("Escape")
                time.sleep(1 + attempt)
        if not resolved_thinking:
            raise RuntimeError(
                "ChatGPT could not prove an allowed reasoning level "
                f"for policy {quality!r}; visible options: {available}"
            )
        disabled = disable_external_tools(page)
        return {
            "quality_policy": quality,
            "requested_model": model,
            "model": resolved_model,
            "requested_thinking": thinking,
            "thinking": resolved_thinking,
            "external_tools": "off",
            "disabled_external_tools": disabled,
            "visible_picker_options": available,
        }

    def attach(self, page, paths: list[str]) -> None:
        file_input = page.locator(chat_backend.SEL["file_input"]).first
        if not file_input.count():
            plus = page.locator(chat_backend.SEL["composer_plus"]).first
            if not plus.count():
                raise RuntimeError("ChatGPT has neither a file input nor an attachment button")
            plus.click(timeout=10000)
            file_input = page.locator(chat_backend.SEL["file_input"]).first
            file_input.wait_for(state="attached", timeout=10000)
        file_input.set_input_files(paths)
        selected = file_input.evaluate(
            "el => Array.from(el.files || []).map(file => file.name)"
        )
        expected = [Path(path).name for path in paths]
        if selected != expected:
            raise RuntimeError(
                f"ChatGPT selected the wrong attachment list: expected {expected}, got {selected}"
            )

    def verify_attachments(self, page, paths: list[str]) -> None:
        wait_for_attachment_names(page, paths)

    def send(self, page, prompt: str) -> None:
        composer = chat_backend._click_composer(page)
        try:
            composer.evaluate("el => el.focus()")
            page.keyboard.insert_text(prompt)
        except Exception:
            composer.fill(prompt)
        inserted = (composer.inner_text() or "").strip()
        if len(inserted) < min(100, len(prompt.strip())):
            raise RuntimeError(
                "ChatGPT composer did not retain the full prompt before Send"
            )
        before_users = page.locator(CHATGPT_USER).count()

        def wait_for_acceptance(timeout_s: int) -> bool:
            accepted_deadline = time.time() + timeout_s
            while time.time() < accepted_deadline:
                if re.search(r"/c/[^/?#]+", page.url or ""):
                    log("ChatGPT accepted Send and assigned a conversation URL")
                    return True
                if page.locator(CHATGPT_USER).count() > before_users:
                    log("ChatGPT accepted Send and rendered the user turn")
                    return True
                try:
                    if not (composer.inner_text() or "").strip():
                        log("ChatGPT accepted Send and cleared the composer")
                        return True
                except Exception:
                    pass
                time.sleep(0.5)
            return False

        time.sleep(1)
        log("ChatGPT submit probe before click: " + repr(_chatgpt_submit_probe(page)))
        sends = page.locator(
            "button[data-testid='send-button'][aria-label='Send prompt'], "
            + chat_backend.SEL["send"]
        )
        clicked = False
        if sends.count():
            send = sends.last
            try:
                send.click(force=True, timeout=10000)
                clicked = True
                log("force-clicked the last ChatGPT Send prompt button")
                log(
                    "ChatGPT submit probe after force-click: "
                    + repr(_chatgpt_submit_probe(page))
                )
            except Exception:
                pass
        if not clicked:
            clicked_details = page.evaluate(
                """() => {
                    const buttons = Array.from(document.querySelectorAll(
                        'button[data-testid="send-button"], button[aria-label="Send prompt"]'
                    ));
                    for (const button of buttons) {
                        const rect = button.getBoundingClientRect();
                        if (button.disabled || !rect.width || !rect.height) continue;
                        if (rect.x < 0 || rect.y < 0 || rect.x >= window.innerWidth ||
                            rect.y >= window.innerHeight) continue;
                        button.click();
                        return {
                            aria: button.getAttribute('aria-label'),
                            testid: button.getAttribute('data-testid'),
                            x: Math.round(rect.x), y: Math.round(rect.y)
                        };
                    }
                    return null;
                }"""
            )
            if clicked_details:
                clicked = True
                log("clicked visible ChatGPT Send button: " + repr(clicked_details))
        if not clicked:
            viewport = page.evaluate(
                "() => ({width: window.innerWidth, height: window.innerHeight})"
            )
            page.mouse.click(
                viewport["width"] * 0.80,
                viewport["height"] * 0.61,
            )
            log(
                "clicked the visible ChatGPT Send-arrow position at "
                f"({viewport['width'] * 0.80:.0f}, {viewport['height'] * 0.61:.0f})"
            )

        if wait_for_acceptance(20):
            return

        # A Playwright click returning without error is not proof that ChatGPT's
        # app accepted it. The image worker already uses Enter as its fallback;
        # do the same here, but only after the acceptance gate proves the click
        # was a no-op so one turn cannot be submitted twice.
        composer.evaluate("el => el.focus()")
        page.keyboard.press("Enter")
        log("ChatGPT click was not accepted; pressed Enter in the focused composer")
        log(
            "ChatGPT submit probe after Enter fallback: "
            + repr(_chatgpt_submit_probe(page))
        )
        if wait_for_acceptance(20):
            return

        debug_path = (
            REPO_ROOT
            / "output"
            / "creative_browser_debug"
            / "chatgpt-send-not-accepted.png"
        )
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(debug_path), full_page=False)
        log("ChatGPT submit probe on rejection: " + repr(_chatgpt_submit_probe(page)))
        raise RuntimeError(
            "ChatGPT did not accept Send within 20s; screenshot: " + str(debug_path)
        )

    def answer_count(self, page) -> int:
        try:
            return page.locator(CHATGPT_ASSISTANT).count()
        except Exception:
            return 0

    def answer_text(self, page) -> str:
        try:
            items = page.locator(CHATGPT_ASSISTANT)
            return items.last.inner_text() if items.count() else ""
        except Exception:
            return ""

    def is_running(self, page) -> bool:
        return _visible(page, chat_backend.SEL["stop_btn"])

    def completion_state(self, page) -> bool | None:
        conversation_id = chat_backend._conversation_id(page)
        if not conversation_id or chat_backend._is_provisional_cid(conversation_id):
            return None
        status = chat_backend._stream_status(page, conversation_id)
        if not status:
            return None
        return str(status).upper() == "COMPLETE"

    def extract(self, page) -> tuple[str, str]:
        selectors = (
            "button[data-testid='copy-turn-action-button']",
            "button[aria-label*='Copy' i]",
        )
        for selector in selectors:
            try:
                buttons = page.locator(selector)
                if not buttons.count():
                    continue
                buttons.last.click(timeout=8000)
                time.sleep(1)
                text = page.evaluate("async () => await navigator.clipboard.readText()")
                if text and len(text.strip()) >= 100:
                    return text, "clipboard"
            except Exception:
                pass
        try:
            text = page.evaluate(gemini_worker.HTML_TO_MD_JS, CHATGPT_ASSISTANT)
            if text and len(text.strip()) >= 100:
                return text, "html"
        except Exception:
            pass
        text = self.answer_text(page)
        if text and len(text.strip()) >= 100:
            return text, "dom"
        raise RuntimeError("ChatGPT answer finished but no extraction path returned text")

    def conversation_url(self, page) -> str:
        return page.url or ""


def run_turn(
    page,
    adapter,
    paths: list[str],
    prompt: str,
    model: str | None,
    quality: str,
    thinking: str | None,
    timeout_s: int,
    on_sent=None,
):
    adapter.new_chat(page)
    resolved_settings = adapter.select_settings(page, model, quality, thinking)
    log(
        f"selected {adapter.name} settings: model={resolved_settings['model']!r}, "
        f"thinking={resolved_settings['thinking']!r}, external tools=off"
    )
    adapter.attach(page, paths)
    adapter.verify_attachments(page, paths)
    before_count = adapter.answer_count(page)
    adapter.send(page, prompt)
    if on_sent:
        last_url = ""
        url_deadline = time.time() + 20
        while time.time() < url_deadline:
            current_url = adapter.conversation_url(page)
            if current_url != last_url:
                on_sent(resolved_settings, current_url)
                last_url = current_url
            if re.search(r"/(?:c|app)/[^/?#]+", current_url or ""):
                break
            time.sleep(0.5)
    wait_for_answer(page, adapter, before_count, timeout_s)
    answer, extraction = adapter.extract(page)
    return answer, extraction, resolved_settings, adapter.conversation_url(page)


def resume_turn(page, adapter, url: str, settings: dict, timeout_s: int):
    page.goto(url, wait_until="domcontentloaded", timeout=45000)
    time.sleep(3)
    wait_for_answer(page, adapter, 0, timeout_s)
    answer, extraction = adapter.extract(page)
    return answer, extraction, settings, adapter.conversation_url(page)


def existing_result_settings(out_path: str | Path, resume_url: str | None) -> dict:
    if not resume_url:
        return {}
    output = Path(out_path)
    metadata = None
    for candidate in (
        output.with_suffix(output.suffix + ".meta.json"),
        output.with_suffix(output.suffix + ".pending.json"),
    ):
        try:
            metadata = json.loads(candidate.read_text(encoding="utf-8"))
            break
        except (OSError, ValueError):
            continue
    if metadata is None:
        return {
            "quality_policy": "preserved from resumed conversation",
            "model": "preserved from resumed conversation",
            "thinking": "preserved from resumed conversation",
        }
    if metadata.get("conversation_url") != resume_url:
        raise RuntimeError(
            "--resume-url does not match the conversation in the existing result metadata"
        )
    return metadata.get("resolved_settings") or {}


def _safe_email(email: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", email.strip().lower())


def run_gemini(
    args, manifest: dict, paths: list[str], resume_settings: dict, on_sent=None
):
    if args.email:
        gemini_video.use_account(args.email)
    if args.firefox:
        if args.profile_dir:
            gemini_worker.FF_PROFILE_DIR = os.path.abspath(args.profile_dir)
            os.makedirs(gemini_worker.FF_PROFILE_DIR, exist_ok=True)
            args.no_reseed = True
            log(f"Gemini uses explicit profile {gemini_worker.FF_PROFILE_DIR}")
        elif args.email:
            gemini_worker.FF_PROFILE_DIR = os.path.join(
                str(BASE_DIR), f".gemini_creative_ff_run_{_safe_email(args.email)}_{os.getpid()}"
            )
        if not args.no_reseed:
            if not gemini_worker.pull_firefox_profile(args.email):
                log("Gemini profile pull did not prove a session; the browser login ladder will decide")
        playwright_factory = gemini_worker.firefox_playwright()
    else:
        playwright_factory = gemini_video._import_playwright()

    with playwright_factory() as playwright:
        context, page = (
            gemini_worker.launch_firefox(playwright, headless=args.headless)
            if args.firefox
            else gemini_video.launch(playwright, headless=args.headless)
        )
        try:
            gemini_worker.ensure_session(page, args.email, firefox=args.firefox)
            if args.resume_url:
                return resume_turn(
                    page,
                    GeminiAdapter(),
                    args.resume_url,
                    resume_settings,
                    args.answer_timeout,
                )
            return run_turn(
                page,
                GeminiAdapter(),
                paths,
                manifest["message"],
                args.model or os.environ.get("GEMINI_CREATIVE_MODEL") or gemini_worker.DEFAULT_MODEL,
                args.quality,
                args.thinking,
                args.answer_timeout,
                on_sent,
            )
        finally:
            try:
                context.close()
            except Exception:
                pass


def configure_chatgpt(args) -> None:
    chat_backend.set_browser_mode("firefox" if args.firefox else "chrome")
    if args.firefox:
        os.environ["FIREFOX_HEADLESS"] = "1" if args.headless else "0"
    if args.profile_dir:
        chat_backend.PROFILE_DIR = os.path.abspath(args.profile_dir)
    elif args.email:
        prefix = ".chatgpt_profile_firefox_" if args.firefox else ".chatgpt_profile_"
        chat_backend.PROFILE_DIR = os.path.join(
            str(BASE_DIR), f"{prefix}{_safe_email(args.email)}"
        )
    os.makedirs(chat_backend.PROFILE_DIR, exist_ok=True)
    chat_backend.COOKIES_FILE = os.path.join(str(BASE_DIR), ".chatgpt_cookies_unused")

    if args.email and args.firefox:
        chat_backend.CHROME_GOLDEN_DIR = os.path.join(
            str(BASE_DIR), f".chatgpt_chrome_golden_{_safe_email(args.email)}"
        )
    if args.email and not args.no_reseed:
        try:
            import chatgpt_session_pull

            target = (
                chat_backend.CHROME_GOLDEN_DIR
                if args.firefox
                else chat_backend.PROFILE_DIR
            )
            if chatgpt_session_pull.pull_chatgpt_session(args.email, target):
                log("ChatGPT session copied from the existing Chrome-Beta source")
            else:
                log("ChatGPT session pull found nothing; the saved-profile login ladder will decide")
        except Exception as exc:
            log(f"ChatGPT session pull failed ({type(exc).__name__}); using the saved-profile ladder")


def run_chatgpt(
    args, manifest: dict, paths: list[str], resume_settings: dict, on_sent=None
):
    configure_chatgpt(args)
    playwright_factory, _ = chat_worker._import_playwright()
    with playwright_factory() as playwright:
        launched = chat_worker.launch_logged_in(playwright, args.email)
        if launched is None:
            raise RuntimeError("ChatGPT login ladder did not produce the requested account session")
        context, page = launched
        try:
            if args.resume_url:
                return resume_turn(
                    page,
                    ChatGPTAdapter(),
                    args.resume_url,
                    resume_settings,
                    args.answer_timeout,
                )
            return run_turn(
                page,
                ChatGPTAdapter(),
                paths,
                manifest["message"],
                args.model or os.environ.get("CHATGPT_CREATIVE_MODEL") or None,
                args.quality,
                args.thinking,
                args.answer_timeout,
                on_sent,
            )
        finally:
            try:
                context.close()
            except Exception:
                pass


def write_result(
    out_path: str | Path,
    answer: str,
    *,
    manifest: dict,
    provider: str,
    requested_model: str | None,
    requested_quality: str,
    requested_thinking: str | None,
    resolved_settings: dict,
    extraction: str,
    conversation_url: str,
) -> list[str]:
    output = Path(out_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    cleaned = clean_answer(answer)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(cleaned, encoding="utf-8")
    temporary.replace(output)
    missing = missing_headings(cleaned, manifest["required_headings"])
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "requested_model": requested_model,
        "requested_quality": requested_quality,
        "requested_thinking": requested_thinking,
        "resolved_settings": resolved_settings,
        "pack_sha256": manifest["pack_sha256"],
        "attachments": manifest["files"],
        "conversation_url": conversation_url,
        "extraction": extraction,
        "answer_sha256": sha256_bytes(cleaned.encode("utf-8")),
        "missing_headings": missing,
    }
    meta_path = output.with_suffix(output.suffix + ".meta.json")
    meta_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    log(f"saved {output} and {meta_path}")
    pending_path = output.with_suffix(output.suffix + ".pending.json")
    pending_path.unlink(missing_ok=True)
    return missing


def write_pending(
    out_path: str | Path,
    *,
    manifest: dict,
    provider: str,
    resolved_settings: dict,
    conversation_url: str,
) -> None:
    """Save enough state to resume a sent turn after a local interruption."""
    output = Path(out_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    pending_path = output.with_suffix(output.suffix + ".pending.json")
    payload = {
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "pack_sha256": manifest["pack_sha256"],
        "attachments": manifest["files"],
        "resolved_settings": resolved_settings,
        "conversation_url": conversation_url,
        "resumable": bool(
            re.search(r"/(?:c|app)/[^/?#]+", conversation_url or "")
        ),
    }
    temporary = pending_path.with_suffix(pending_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(pending_path)
    log(f"saved pending checkpoint {pending_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("gemini", "chatgpt"), required=True)
    parser.add_argument("--pack", required=True, help="pack directory or MANIFEST.json")
    parser.add_argument("--out", required=True, help="raw Markdown answer path")
    parser.add_argument("--email", help="account expected in the selected provider")
    parser.add_argument("--model", help="visible model-picker label; provider default when omitted")
    parser.add_argument(
        "--quality",
        choices=("best", "balanced", "fast"),
        default="best",
        help="provider-specific reasoning policy; defaults to strongest available",
    )
    parser.add_argument(
        "--thinking",
        help="exact visible reasoning/thinking label; overrides the quality policy",
    )
    parser.add_argument("--profile-dir", help="explicit persistent browser profile")
    parser.add_argument(
        "--resume-url",
        help="resume an already-sent deferred conversation without uploading or sending again",
    )
    parser.add_argument("--no-reseed", action="store_true", help="do not refresh from the existing browser session source")
    parser.add_argument("--answer-timeout", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--dry-run", action="store_true", help="verify and list the pack without opening a browser")
    engine = parser.add_mutually_exclusive_group()
    engine.add_argument("--firefox", dest="firefox", action="store_true", default=True)
    engine.add_argument("--chrome", dest="firefox", action="store_false")
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--headless", dest="headless", action="store_true", default=True)
    window.add_argument("--show-window", dest="headless", action="store_false")
    args = parser.parse_args()

    if not args.dry_run and not args.email:
        parser.error("--email is required for a live run so the worker can reject a wrong account")
    manifest, paths = load_pack(args.pack)
    log(
        f"verified pack {manifest['pack_sha256']} with {len(paths)} attachments "
        f"for {args.provider}"
    )
    for path in paths:
        log(f"  attach {Path(path).relative_to(REPO_ROOT)}")
    if args.dry_run:
        return 0

    resume_settings = existing_result_settings(args.out, args.resume_url)
    on_sent = None
    if not args.resume_url:
        on_sent = lambda settings, url: write_pending(
            args.out,
            manifest=manifest,
            provider=args.provider,
            resolved_settings=settings,
            conversation_url=url,
        )
    if args.provider == "gemini":
        answer, extraction, resolved_settings, conversation_url = run_gemini(
            args, manifest, paths, resume_settings, on_sent
        )
    else:
        answer, extraction, resolved_settings, conversation_url = run_chatgpt(
            args, manifest, paths, resume_settings, on_sent
        )
    missing = write_result(
        args.out,
        answer,
        manifest=manifest,
        provider=args.provider,
        requested_model=args.model,
        requested_quality=args.quality,
        requested_thinking=args.thinking,
        resolved_settings=resolved_settings,
        extraction=extraction,
        conversation_url=conversation_url,
    )
    if missing:
        log("answer saved but failed the requested shape; missing: " + ", ".join(missing))
        return 2
    log("answer contains every required heading")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
