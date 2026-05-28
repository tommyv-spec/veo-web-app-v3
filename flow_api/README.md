# flow_api — stable Flow worker (private-API path)

Drive Google Flow by calling its private JSON API **from inside the logged-in
Patchright page**, instead of clicking DOM buttons and scraping `data-index` tiles.

**Why:** the DOM path mis-attributes clips (newest-tile-by-index guess drifts → wrong
clip → stuck/redo bugs) and breaks whenever Google moves a button. The API path reads
the clip's real `media_id` UUID straight from the submit response, so a finished clip is
always the one we submitted, and there are no buttons to break.

**Status:** module + capture hook BUILT. Registered as an additional backend mode
(`BackendType.FLOW_API`). Generate-seam wiring into the live worker is the remaining
step — do it AFTER the capture run (below), so we wire once against confirmed model keys
and can verify live. `FLOW_API_MODE` defaults `off`.

## Modes (this is an ADDITIONAL, removable mode)

`backends/selector.py` `BackendType`: `api` (Veo direct), `flow` (DOM clicks + tile
scrape), **`flow_api` (NEW — same logged-in Flow session, private API in-page)**,
`higgsfield`. The worker is "launched in flow_api mode" by setting `FLOW_API_MODE=on`;
per clip it tries the API path and falls back to the DOM path on any failure.

**Launch switches (env):**
- `FLOW_API_CAPTURE=1` — record real submit bodies/model keys (read-only; step 1 below).
- `FLOW_API_MODE=on` — run the API generation path (after model_map.json is filled).

**To remove this mode entirely later:** delete the `flow_api/` package, the
`BackendType.FLOW_API` enum member + `worker_flow_api_enabled()` in `selector.py`, the
`FLOW_API_MODE` line in `config.py`, and the capture hook in `static/flow_worker.py`
(`_install_flow_api_capture` + its one call site). Nothing else depends on it.

Sources: ported from FlowKit (`crisng95/flowkit`), cross-checked vs useapi.net's
documented Flow API + multiple independent extensions. Same private API, multiple
implementations → stable surface.

---

## How it works (no extension needed — Patchright runs in-page)

FlowKit uses a Chrome extension to run the fetch + captcha inside the page. We already
have a logged-in page via Patchright, so we use `page.evaluate` instead. Per clip:

1. **token** — `page.on("request")` sniffs the `ya29.*` bearer off the page's own calls (`page_ops.install_token_capture`).
2. **upload** — frame bytes → base64 → `POST /v1/flow/uploadImage` → `media_id` UUID (no captcha).
3. **captcha** — `grecaptcha.enterprise.execute(SITE_KEY, {action})` minted in-page, free (`page_ops.mint_captcha`).
4. **submit** — `POST /v1/video:batchAsyncGenerateVideoStartImage` (or `...StartAndEndImage`) with frame id(s) + prompt + model key + captcha token → response carries the clip's `media_id` UUID.
5. **poll** — `POST /v1/video:batchCheckAsyncVideoGenerationStatus` until `SUCCESSFUL`/`FAILED`.
6. **resolve** — finished URL from the poll/`get_media`; existing cookie downloader fetches bytes.

## Files

| File | Role |
|---|---|
| `config.py` | endpoints, API key, reCAPTCHA site key, tuning, model-key map + `resolve_model_key()` |
| `builders.py` | pure request-body builders + `inject_captcha_token` (unit-tested) |
| `parsing.py` | response parsing, `media_id` UUID attribution (unit-tested) |
| `page_ops.py` | in-page primitives: token sniff, captcha mint, `api_fetch` via `page.evaluate` |
| `client.py` | `FlowApiClient` — upload/submit/poll/resolve + cooldown + captcha retry |
| `adapter.py` | `generate_clip_via_api(page, ...)` — the one seam the worker calls |
| `capture_helper.py` | attach to page to record real submit bodies/model keys from live traffic |
| `tests/` | offline unit tests (no browser/network) — `python -m pytest flow_api/tests/ -q` |

## Image generation (HAR-confirmed 2026-05-28)

The image side is now CONFIRMED end-to-end from a HAR capture:

- Endpoint: `POST /v1/projects/{project_id}/flowMedia:batchGenerateImages` (captcha: `IMAGE_GENERATION`).
- `imageModelName`: `NARWHAL` = **Nano Banana 2** (confirmed). `GEM_PIX_2` = **Nano Banana Pro** (from FlowKit, not in this HAR — confirm if used).
- Per-request keys (plain t2i): `imageAspectRatio, imageModelName, seed, structuredPrompt`.
- With reference/base images: add `imageInputs: [{name, imageInputType: IMAGE_INPUT_TYPE_BASE_IMAGE | IMAGE_INPUT_TYPE_REFERENCE}]` and top-level `mediaGenerationContext.batchId` + `useNewMedia: true`.
- `uploadImage` confirmed identical to video: `POST /v1/flow/uploadImage`, projectId in clientContext, no captcha.
- Image submit returns the finished media in the same response (no separate poll loop) — read `data.media[0].name` (UUID) and `.image.generatedImage.fifeUrl`.

Built: `builders.build_generate_image`, `parsing.extract_image_media_id` / `extract_image_url`, `client.submit_image`, `adapter.generate_image_via_api`. Capture hook mirrored into `image_worker.py` (same env gate, same JSONL output).

## BEFORE enabling — confirm the unknowns (doc-grounded discipline)

Three things are NOT yet confirmed for this account and are marked `NEEDS_CAPTURE`
in `config.py`:
1. exact `videoModelKey` for **Veo 3.1 Lite** and **Quality** (Fast + Lite-Lower-Priority are filled).
2. **Omni Flash** Ingredients-mode submit endpoint + body (Omni is NOT a start-frame submit).

To fill them:
```python
from flow_api.capture_helper import attach_capture
attach_capture(page, out_path="flow_api_capture.jsonl")
# then do ONE manual generate per model/mode in the logged-in Flow tab
```
Read `flow_api_capture.jsonl` → copy each `videoModelKey` + endpoint into
`flow_api/model_map.json` (see `model_map.example.json`).

## Wiring into the worker (when ready — NOT done yet)

In the per-clip generate step (`backends/flow_backend.py`, where it currently uploads
frames and `flow_worker.click_generate_button` clicks `arrow_forward`):

```python
from config import FLOW_API_MODE
if FLOW_API_MODE == "on":
    try:
        from flow_api.adapter import generate_clip_via_api
        result = await generate_clip_via_api(
            page, prompt=clip.prompt,
            start_image_bytes=open(clip.start_frame_path, "rb").read(),
            end_image_bytes=(open(clip.end_frame_path, "rb").read() if clip.end_frame_path else None),
            model_name=page._veo_model, scene_id=clip.scene_id, project_id=project_id,
        )
        log(f"path=api scene={clip.scene_id} media_id={result['media_id']}")
        # use result['media_id'] / result['url'] for attribution + download
    except Exception as e:
        log(f"path=dom (api fell back) scene={clip.scene_id} reason={e}")
        # ... existing DOM-click path unchanged ...
else:
    # ... existing DOM-click path unchanged ...
```

Add the `path=api|dom` + `media_id` diagnostic log line so the first live run proves
which path executed and that attribution is correct (verification-before-completion).

## Rollback
Known-good deployed state is captured in memory (`project_rollback-point` → v763,
root `53aafda` / code `2ee642b`). Roll back there if a deploy of this misbehaves.
