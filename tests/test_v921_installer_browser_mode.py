"""v921 — a fresh image-worker install starts on Firefox.

reCAPTCHA scores the ENGINE, not the profile: measured 2026-08-07, Camoufox
minted 10/10 real 0cA tokens while Chrome minted ~0% the same day (see
static/browser_driver.py). The worker already supports Firefox end to end
(img-v591: camoufox bootstrap, per-mode session/golden dirs, firefox_profile_pull),
but nothing selected it — the installer never wrote BROWSER_MODE, so every
install fell through to the Chrome default.

The switch belongs in config, not code: browser_driver states the invariant as
"default-by-config, never default-by-code", so an existing worker is never
migrated by a code change. Only the .env this installer writes picks Firefox.
"""
import image_platform as ip


def _env_lines(installer_text):
    return [l.strip() for l in installer_text.splitlines() if "BROWSER_MODE" in l]


def test_windows_installer_writes_firefox_by_default():
    txt = ip._generate_image_windows_installer(
        "KEY", "https://kavenobuilder.com", parallel_slots=2, laptop_email="a@b.com")
    lines = _env_lines(txt)
    assert lines, "windows installer never writes BROWSER_MODE"
    assert "firefox" in lines[0]


def test_unix_installer_writes_firefox_by_default():
    txt = ip._generate_image_unix_installer(
        "KEY", "https://kavenobuilder.com", parallel_slots=2, laptop_email="a@b.com")
    lines = _env_lines(txt)
    assert lines, "unix installer never writes BROWSER_MODE"
    assert "firefox" in lines[0]


def test_chrome_is_still_reachable_on_request():
    for gen in (ip._generate_image_windows_installer, ip._generate_image_unix_installer):
        lines = _env_lines(gen("KEY", "u", browser_mode="stealth"))
        assert lines and "stealth" in lines[0]
        assert "firefox" not in lines[0]


def test_browser_mode_sits_in_the_env_block_next_to_the_other_settings():
    txt = ip._generate_image_windows_installer("KEY", "u")
    body = txt[txt.index("PARALLEL_SLOTS"):]
    # same .env write block, before it is redirected to the file
    assert "BROWSER_MODE" in body[:body.index('> "%WORKER_DIR%')]


def test_the_worker_default_is_untouched():
    """The code default must stay Chrome — only config migrates a worker."""
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(ip.__file__).parent / "static"))
    import browser_driver

    assert browser_driver.resolve_browser_mode(env={}) == "stealth"
    assert browser_driver.is_firefox_mode("stealth") is False
    assert browser_driver.is_firefox_mode("firefox") is True
    assert browser_driver.is_firefox_mode("camoufox") is True


def test_banner_matches_the_selected_engine():
    """"Chrome will open minimized" on a headless Firefox install is a lie the
    operator would act on — they told me "it opened chrome"."""
    ff = ip._generate_image_windows_installer("K", "u")
    assert "headless" in ff and "minimized" not in ff

    chrome = ip._generate_image_windows_installer("K", "u", browser_mode="stealth")
    assert "minimized" in chrome


def test_reset_wipes_the_profiles_the_selected_engine_actually_uses():
    """Chrome and Firefox profiles live in separate trees, so wiping only the
    chrome dirs made reset=1 a silent no-op on a Firefox install."""
    ff = ip._generate_image_windows_installer("K", "u", reset=True)
    assert "image-firefox-session" in ff and "image-firefox-golden" in ff
    assert "image-chrome-session" in ff          # chrome leftovers cleared too

    chrome = ip._generate_image_windows_installer("K", "u", reset=True,
                                                  browser_mode="stealth")
    assert "image-firefox-session" not in chrome
    assert "image-chrome-session" in chrome


def test_installer_endpoint_offers_only_known_modes():
    import inspect
    src = inspect.getsource(ip.download_image_worker_installer)
    assert 'browser_mode: str = Query("firefox"' in src
    assert "^(firefox|camoufox|stealth|chrome)$" in src
