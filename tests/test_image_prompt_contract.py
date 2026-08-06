import image_prompt_contract as contract


def _refs():
    return [
        {"role": "bottle", "reference_class": "product", "slot_order": 2},
        {"role": "Nuri", "reference_class": "persona", "slot_order": 0},
        {"role": "previous kitchen frame", "reference_class": "chain", "reference_intent": "continuity", "slot_order": 1},
        {"role": "lighting style", "reference_class": "other", "slot_order": 3},
    ]


def test_contract_numbers_references_in_actual_slot_order():
    prompt = contract.build_image_prompt_contract(
        "A woman raises the bottle in a bright kitchen.",
        _refs(),
        "9:16",
        backend="banana",
    )

    assert prompt.startswith(contract.PROMPT_CONTRACT_MARKER)
    assert prompt.index("Image 1 - Role: Nuri.") < prompt.index("Image 2 - Role: previous kitchen frame.")
    assert prompt.index("Image 2 - Role: previous kitchen frame.") < prompt.index("Image 3 - Role: bottle.")
    assert prompt.index("Image 3 - Role: bottle.") < prompt.index("Image 4 - Role: lighting style.")
    assert "defining facial features" in prompt
    assert "Continuity or base-scene reference" in prompt
    assert "packaging, label, colors, logo" in prompt
    assert "Do not borrow unrelated traits" in prompt
    assert "Image numbering below matches the attachment order." in prompt
    assert prompt.count("Use (fallback):") == 4


def test_contract_keeps_fourteen_references_in_order_without_truncation():
    refs = [
        {
            "role": f"role {i}",
            "reference_class": "other",
            "reference_instruction": f"Take only feature {i}.",
            "slot_order": i,
        }
        for i in range(14)
    ]
    prompt = contract.build_image_prompt_contract("Scene body.", refs)

    assert prompt.count("Use (authoritative):") == 14
    assert "Image 14 - Role: role 13." in prompt
    assert prompt.index("Image 1 - Role: role 0.") < prompt.index("Image 14 - Role: role 13.")


def test_external_role_prefix_is_not_exposed_to_image_models():
    prompt = contract.build_image_prompt_contract(
        "Scene body.",
        [{
            "role": "external:background geometry",
            "reference_class": "other",
            "reference_instruction": "Take only the porch layout.",
            "slot_order": 0,
        }],
    )

    assert "Role: background geometry." in prompt
    assert "external:" not in prompt


def test_open_reference_instruction_overrides_class_fallback():
    prompt = contract.build_image_prompt_contract(
        "Put the couple on the porch while keeping the new camera angle.",
        [{
            "role": "porch inspiration",
            "reference_class": "chain",
            "reference_intent": "continuity",
            "reference_instruction": (
                "Take only the porch geometry and railing design for the background. "
                "Ignore the people, clothing, lighting, and camera angle."
            ),
            "slot_order": 0,
        }],
    )

    assert "Take only the porch geometry and railing design" in prompt
    assert "Ignore the people, clothing, lighting, and camera angle." in prompt
    assert "Continuity or base-scene reference" not in prompt
    assert "Use (authoritative):" in prompt
    assert "An authoritative Use line wins over any conflicting scene instruction." in prompt
    assert "cannot override an authoritative Use line" in prompt


def test_contract_keeps_scene_body_and_removes_only_old_manifest_line():
    body = """Use Image 1 for the main character.

Photorealistic scene.
Put the scarf from Image 2 on the woman in Image 1.
Do not change the kitchen."""
    prompt = contract.build_image_prompt_contract(
        body,
        [{"role": "main character", "reference_class": "persona", "slot_order": 0}],
        "16:9",
    )

    assert prompt.count("Use Image 1 for the main character.") == 0
    assert "Put the scarf from Image 2 on the woman in Image 1." in prompt
    assert "Do not change the kitchen." in prompt
    assert "Aspect ratio: horizontal 16:9." in prompt


def test_chatgpt_contract_has_one_trigger_and_supports_open_aspect_values():
    prompt = contract.build_image_prompt_contract(
        "A clean product close-up.",
        [],
        "4:5",
        backend="chatgpt",
    )

    assert prompt.startswith("Crea immagine:\n" + contract.PROMPT_CONTRACT_MARKER)
    assert prompt.count("Crea immagine:") == 1
    assert "REFERENCE IMAGES" not in prompt
    assert "Aspect ratio: 4:5." in prompt


def test_internal_chain_role_names_are_humanized():
    prompt = contract.build_image_prompt_contract(
        "Scene body.",
        [
            {"role": "variant_chain:Nuri_before", "reference_class": "persona", "slot_order": 0},
            {"role": "chain_from_image_3", "reference_class": "chain", "slot_order": 1},
        ],
    )

    assert "Image 1 - Role: Nuri before." in prompt
    assert "Image 2 - Role: the prior scene from image 3." in prompt
    assert "variant_chain:" not in prompt
    assert "chain_from_image_" not in prompt


def test_second_chain_can_be_body_reference_without_claiming_the_full_scene():
    prompt = contract.build_image_prompt_contract(
        "Keep the kitchen from Image 1. Use the pose from Image 2.",
        [
            {"role": "prior scene", "reference_class": "chain", "reference_intent": "continuity", "slot_order": 0},
            {"role": "body reference", "reference_class": "chain", "reference_intent": "body", "slot_order": 1},
        ],
    )

    assert "Continuity or base-scene reference for prior scene" in prompt
    assert "Body or pose reference for body reference" in prompt
    assert "Do not replace the main identity, face, product, setting, camera" in prompt
    assert prompt.count("Continuity or base-scene reference") == 1


def test_worker_uses_marked_contract_without_legacy_duplicate():
    import image_worker

    marked = contract.build_image_prompt_contract(
        "Scene body.",
        [{"role": "main character", "reference_class": "persona", "slot_order": 0}],
        "9:16",
    )
    prepared = image_worker._prepare_reference_prompt(marked, ["main_character.png"])

    assert prepared == marked
    assert prepared.count("IMAGE REFERENCE CONTRACT v2") == 1
    assert "Use Image 1 for main character." not in prepared


def test_worker_keeps_legacy_fallback_for_unmarked_jobs():
    import image_worker

    prepared = image_worker._prepare_reference_prompt(
        "A person in a kitchen.",
        ["main_character.png", "product.png"],
    )

    assert prepared.startswith(
        "Use Image 1 for main character.\nUse Image 2 for product.\n\n"
    )
    assert prepared.endswith("A person in a kitchen.")


def test_worker_accepts_queued_v1_contract_without_duplicate_manifest():
    import image_worker

    legacy = "IMAGE REFERENCE CONTRACT v1\n\nSCENE TO CREATE\nScene body."
    assert image_worker._prepare_reference_prompt(legacy, ["ref.png"]) == legacy


def test_reference_upload_metadata_matches_file_bytes():
    import image_worker

    assert image_worker._reference_upload_metadata(
        "wrong.jpg", b"\x89PNG\r\n\x1a\nrest", 0
    ) == ("ref_1.png", "image/png")
    assert image_worker._reference_upload_metadata(
        "wrong.png", b"\xff\xd8\xffrest", 1
    ) == ("ref_2.jpg", "image/jpeg")
    assert image_worker._reference_upload_metadata(
        "wrong.jpg", b"RIFFxxxxWEBPrest", 2
    ) == ("ref_3.webp", "image/webp")


def test_reference_downloads_are_sorted_by_slot(monkeypatch, tmp_path):
    import image_worker
    import requests

    seen_urls = []

    class Response:
        status_code = 200
        ok = True

        def iter_content(self, chunk_size=8192):
            yield b"image bytes"

    def fake_get(url, **kwargs):
        seen_urls.append(url)
        return Response()

    monkeypatch.setattr(requests, "get", fake_get)
    results, missing = image_worker._download_reference_inputs(
        "key",
        [
            {"url": "https://example.test/second", "filename": "b.png", "role": "second", "slot_order": 1},
            {"url": "https://example.test/first", "filename": "a.png", "role": "first", "slot_order": 0},
        ],
        str(tmp_path),
    )

    assert missing == []
    assert seen_urls == ["https://example.test/first", "https://example.test/second"]
    assert [item["role"] for item in results] == ["first", "second"]


def test_chatgpt_attach_waits_for_all_previews_in_file_order(monkeypatch):
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
    import chatgpt_image_backend as backend

    class FileInput:
        def __init__(self):
            self.paths = []

        def set_input_files(self, paths):
            self.paths = list(paths)

        def evaluate(self, script):
            return [os.path.basename(path) for path in self.paths]

    class Locator:
        def __init__(self, file_input):
            self.first = file_input

    class Page:
        def __init__(self):
            self.file_input = FileInput()

        def locator(self, selector):
            return Locator(self.file_input)

    src_calls = iter([[], ["uploaded-one", "uploaded-two"]])
    monkeypatch.setattr(backend, "_all_img_srcs", lambda page: next(src_calls))
    monkeypatch.setattr(backend.time, "sleep", lambda seconds: None)
    page = Page()

    backend._attach_reference_files(page, ["C:/refs/one.png", "C:/refs/two.webp"])

    assert page.file_input.paths == ["C:/refs/one.png", "C:/refs/two.webp"]
