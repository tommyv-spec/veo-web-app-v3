import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_isomni",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_is_omni_matches_omni_flash():
    fw = _load()
    assert fw.is_omni("Omni Flash") is True
    assert fw.is_omni("omni flash") is True
    assert fw.is_omni("Omni Flash [Beta]") is True


def test_is_omni_rejects_veo_models():
    fw = _load()
    assert fw.is_omni("Veo 3.1 - Lite [Lower Priority]") is False
    assert fw.is_omni("Veo 3.1 - Fast") is False
    assert fw.is_omni("Veo 3.1 - Quality") is False
    assert fw.is_omni("") is False
    assert fw.is_omni(None) is False
