"""Regression checks for the download-tab wedge seen on 2026-09-19."""

import ast
from pathlib import Path
import textwrap


WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _method(name: str) -> str:
    for node in ast.walk(TREE):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_source_segment(SOURCE, node) or ""
    raise AssertionError(f"{name} not found")


def test_dead_refresh_returns_failure_instead_of_continuing():
    body = _method("_refresh_and_verify")
    wedge = body.index("except _PageWedged:")
    assert "return False" in body[wedge:wedge + 450]


def test_download_loop_replaces_dead_page_before_tile_scan():
    body = _method("_download_loop")
    scan = body.index("CHECK idx 0, 1, 2, 3, 4, 5")
    guarded = body[scan - 1800:]
    first_probe = guarded.index("_locator_attached(container)")
    guard = guarded.index('wait_for_function("1", timeout=3000)')
    replacement = guarded.index("_replace_wedged_page(project_url)", guard)
    assert guard < replacement < first_probe


def test_download_scans_use_only_bounded_locator_probes():
    helper = _method("_locator_attached")
    assert 'wait_for(state="attached", timeout=timeout_ms)' in helper
    assert "if DownloadHelper._is_cdp_disconnect(exc):" in helper
    assert "raise" in helper
    assert "container.count()" not in _method("_scan_all_containers")
    assert "container.count()" not in _method("_download_loop")
    assert "video_check.count()" not in _method("_download_loop")


def test_variant_download_has_no_unbounded_locator_counts():
    body = _method("_download_clip_variants")
    tree = ast.parse(textwrap.dedent(body))
    count_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "count"
    ]
    assert not count_calls
    assert "range(video_count, max(video_count, 8))" in body


def test_deep_scan_keeps_fresh_scan_urls_if_tile_detaches():
    body = _method("_download_loop")
    marker = body.index("The tile can disappear between the bounded probe")
    fallback = body.index("deep_scan_urls = scan_urls", marker)
    assert fallback > marker


def test_nested_download_handlers_propagate_all_disconnects():
    loop = _method("_download_loop")
    deep_error = loop.index("Deep scan download error for clip")
    deep_handler = loop[deep_error - 180:deep_error]
    assert "if self._is_cdp_disconnect(e):" in deep_handler
    assert "raise" in deep_handler

    variants = _method("_download_clip_variants")
    outer = variants.rindex("Error downloading variant")
    outer_handler = variants[outer - 220:outer]
    assert "if self._is_cdp_disconnect(e):" in outer_handler
    assert "raise" in outer_handler
    assert 'any(x in err_str for x in' not in variants

    scan = _method("_scan_all_containers")
    scan_error = scan.index("Scan error:")
    scan_handler = scan[scan_error - 150:scan_error]
    assert "if self._is_cdp_disconnect(e):" in scan_handler
    assert "raise" in scan_handler

    ready_error = loop.index("Error downloading clip")
    ready_handler = loop[ready_error - 220:ready_error]
    assert "if self._is_cdp_disconnect(e):" in ready_handler
    assert "raise" in ready_handler
    assert 'any(x in str(e) for x in' not in loop
    assert '_is_cdp = any(' not in variants


def test_replacement_uses_a_new_tab_and_proves_it_answers():
    body = _method("_replace_wedged_page")
    assert ".context.new_page()" in body
    assert "_navigate_to_project(project_url, max_retries=1)" in body
    assert 'wait_for_function("1", timeout=10000)' in body
    assert "ensure_videos_tab_selected(replacement)" in body
    assert "ensure_batch_view_mode(replacement" in body
    assert "self._is_cdp_disconnect(exc)" in body
    assert "old_page.close(run_before_unload=False)" in body
    assert "replacement.close(run_before_unload=False)" in body


def test_deep_scan_recovers_before_scanning_containers():
    body = _method("_download_loop")
    deep = body.index("DEEP SCAN: Final check")
    tail = body[deep:]
    refresh = tail.index("refreshed = self._refresh_and_verify(project_url)")
    replacement = tail.index("_replace_wedged_page(project_url)", refresh)
    scan = tail.index("_scan_all_containers", replacement)
    assert refresh < replacement < scan


def test_disconnect_classifier_is_used_by_both_outer_download_handlers():
    inner = _method("_process_download_inner")
    dynamic = _method("_download_from_project_dynamic")
    assert "if self._is_cdp_disconnect(e):" in inner
    assert "if self._is_cdp_disconnect(e):" in dynamic
    assert '"browser has been closed"' not in inner
    assert '"browser has been closed"' not in dynamic


def test_new_project_click_never_reports_home_as_a_project():
    body = _method("_fa_or_dom_new_project_click")
    no_project = body.index("clicked New project but no /project/<uuid>")
    refusal = body.index('raise RuntimeError("New project click did not open a Flow project")')
    assert refusal > no_project
    recovery = body.index("project DOM is late; reloading Flow once")
    activity_probe = body.index('activity("waiting for late new-project DOM")')
    assert recovery < activity_probe < refusal
    canonical_refusal = body.index("canonical navigation was not proven")
    final_dom_proof = body.index("_final_dom_pid = _v962_project_id_from_dom(page)")
    assert final_dom_proof < canonical_refusal
