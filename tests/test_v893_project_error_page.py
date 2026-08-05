"""v893 — foreign-project / error-overlay detection (_project_page_ok).

A project created under another account keeps its /project/<uuid> URL but
renders "Something went wrong." — the check must return False (discard +
fresh project), in English and Spanish, and True on a healthy page.
"""
import pytest

import image_worker as iw


class FakePage:
    def __init__(self, url, body_text):
        self.url = url
        self._body = body_text

    def evaluate(self, _js):
        txt = self._body.lower()
        has_ok = any(s in txt for s in ("videos", "scenes", "escenas"))
        if "something went wrong" in txt or "se produjo un error" in txt:
            return "err"
        if ("back to projects" in txt or "volver a los proyectos" in txt) and not has_ok:
            return "err"
        if has_ok:
            return "ok"
        return "wait"


@pytest.fixture(autouse=True)
def _no_cooldown(monkeypatch):
    monkeypatch.setattr(iw, "_FLOW_ERR_COOLDOWN_S", 0)


PROJ = "https://labs.google/fx/tools/flow/project/e9571943-46de-4967-b48f-96296f445116"


def test_error_overlay_on_project_url_is_broken():
    # The exact 2026-08-05 case: URL stays on /project/, overlay says broken.
    page = FakePage(PROJ, "Something went wrong.\nBack to projects")
    assert iw._project_page_ok(page, "test", deadline_s=2.5) is False


def test_spanish_error_overlay_is_broken():
    page = FakePage(PROJ, "Se produjo un error\nVolver a los proyectos")
    assert iw._project_page_ok(page, "test", deadline_s=2.5) is False


def test_redirect_off_project_is_broken():
    page = FakePage("https://labs.google/fx/tools/flow", "Videos Scenes")
    assert iw._project_page_ok(page, "test", deadline_s=2.5) is False


def test_healthy_project_page_is_ok():
    page = FakePage(PROJ, "Scenes\nVideos\nAdd to prompt")
    assert iw._project_page_ok(page, "test", deadline_s=2.5) is True


def test_unclear_page_proceeds_optimistically():
    page = FakePage(PROJ, "loading spinner text")
    assert iw._project_page_ok(page, "test", deadline_s=2.5) is True
