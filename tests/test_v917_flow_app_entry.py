"""v917 — Flow home serves marketing OR the app; the worker must enter the app.

Since v914/v916 the golden ships Google SSO only and strips the labs.google
session, so the first visit after a golden restore lands on Google's public
Flow page. Its "Create with Flow" is a plain <button> with no href, so the old
code clicked it, saw no error, declared "Logged in and ready", and then died
three retries later on "New project button not visible" at
labs.google/fx/tools/flow#capabilities (operator, 2026-08-07).

Live verification of these exact functions from a fresh golden copy:
marketing start -> app entered -> New project clicked -> real project created.
"""
# v962.2 (2026-09-06): the /project mint route is a client-side 404 on the new
# host (measured by session 9e4b16cc), so ensure_flow_app_entered no longer
# visits FLOW_APP_MINT_URL when FLOW_HOME_URL is flow.google.com — each attempt
# is one clean load of home. The expected goto sequences below changed from
# [MINT, HOME] to [HOME] for that reason, not because the entry logic did.
import image_worker as iw


class FakePage:
    """Scripted Flow page: `states` is what _flow_app_rendered will see, in
    order; navigations and CTA clicks are recorded."""

    def __init__(self, states):
        self.states = list(states)
        self.gotos = []
        self.url = iw.FLOW_HOME_URL

    def evaluate(self, _js):
        return self.states.pop(0) if self.states else False

    def goto(self, url, **_kw):
        self.gotos.append(url)
        self.url = url

    def locator(self, _sel):
        raise AssertionError("banner lookup should be guarded")


def _no_sleep(monkeypatch):
    monkeypatch.setattr(iw.time, "sleep", lambda *_a, **_k: None)


def test_already_in_the_app_is_a_no_op(monkeypatch):
    _no_sleep(monkeypatch)
    monkeypatch.setattr(iw, "dismiss_create_with_flow",
                        lambda *a, **k: pytest_fail("must not touch the CTA"))
    page = FakePage([True])
    assert iw.ensure_flow_app_entered(page, "T") is True
    assert page.gotos == []          # no navigation on the happy path


def pytest_fail(msg):
    raise AssertionError(msg)


def test_cta_click_alone_can_enter_the_app(monkeypatch):
    _no_sleep(monkeypatch)
    clicked = []
    monkeypatch.setattr(iw, "dismiss_create_with_flow",
                        lambda *a, **k: clicked.append(True))
    # not rendered -> CTA clicked -> rendered
    page = FakePage([False, True])
    assert iw.ensure_flow_app_entered(page, "T") is True
    assert clicked == [True]
    assert page.gotos == []          # no need for the mint route


def test_mint_route_runs_when_the_cta_leaves_an_empty_shell(monkeypatch):
    _no_sleep(monkeypatch)
    monkeypatch.setattr(iw, "dismiss_create_with_flow", lambda *a, **k: None)
    monkeypatch.setattr(iw, "_dismiss_flow_banner", lambda *a, **k: False)
    # not rendered -> CTA -> still not rendered -> mint + home -> rendered
    page = FakePage([False, False, True])
    assert iw.ensure_flow_app_entered(page, "T") is True
    assert page.gotos == [iw.FLOW_HOME_URL]


def test_gives_up_after_the_attempt_budget(monkeypatch):
    _no_sleep(monkeypatch)
    monkeypatch.setattr(iw, "dismiss_create_with_flow", lambda *a, **k: None)
    monkeypatch.setattr(iw, "_dismiss_flow_banner", lambda *a, **k: False)
    page = FakePage([False] * 12)
    assert iw.ensure_flow_app_entered(page, "T", attempts=2) is False
    # two full rounds, each: mint route + home
    assert page.gotos == [iw.FLOW_HOME_URL] * 2


def test_a_broken_cta_does_not_abort_the_entry(monkeypatch):
    _no_sleep(monkeypatch)

    def boom(*_a, **_k):
        raise RuntimeError("CTA vanished")

    monkeypatch.setattr(iw, "dismiss_create_with_flow", boom)
    monkeypatch.setattr(iw, "_dismiss_flow_banner", lambda *a, **k: False)
    page = FakePage([False, False, True])
    assert iw.ensure_flow_app_entered(page, "T") is True
    assert page.gotos == [iw.FLOW_HOME_URL]


def test_app_detection_survives_a_dead_page():
    class Dead:
        def evaluate(self, _js):
            raise RuntimeError("page closed")

    assert iw._flow_app_rendered(Dead()) is False


def test_mint_url_is_an_app_route_under_flow_home():
    assert iw.FLOW_APP_MINT_URL.startswith(iw.FLOW_HOME_URL.rstrip("/"))
    assert iw.FLOW_APP_MINT_URL.endswith("/project")
