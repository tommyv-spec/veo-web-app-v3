# -*- coding: utf-8 -*-
"""Kling image-to-video via the Higgsfield HTTP API. Opt-in add-on backend.

Contract verified 2026-05-24 against docs.higgsfield.ai:
  POST {BASE_URL}/{slug}  body {image_url, prompt, duration}
       -> {request_id, status_url, video:{url}}
  GET  {status_url}       -> {status, video:{url}}  (poll until status == completed)
Auth: header  Authorization: Key KEY_ID:KEY_SECRET

The existing Veo-via-Google-Flow path is unaffected by this module.
"""
import time
from pathlib import Path
from typing import Optional, Union

import requests

BASE_URL = "https://platform.higgsfield.ai"


def animate_image(
    image_url: str,
    prompt: str,
    credentials: str,
    out_path: Union[str, Path],
    slug: str = "kling-video/v2.1/pro/image-to-video",
    duration: int = 5,
    sound: str = "on",
    poll_interval: float = 5.0,
    max_polls: int = 120,
    timeout: float = 60.0,
) -> Path:
    """Submit an image-to-video job, poll to completion, download the mp4.

    Args:
        image_url: public URL of the start frame (e.g. an R2 public URL).
        prompt: motion prompt.
        credentials: "KEY_ID:KEY_SECRET".
        out_path: where to write the downloaded mp4.
        slug: Higgsfield model slug (defaults to Kling i2v).
        duration: clip seconds.
    Returns:
        out_path (Path) on success.
    Raises:
        RuntimeError if the job ends failed/nsfw/cancelled or never completes.
    """
    headers = {
        "Authorization": f"Key {credentials}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # The official client sends this UA; the platform blocks unknown/default
        # (python-requests) UAs as "browser usage" → 403. Mimic the server SDK.
        "User-Agent": "higgsfield-server-js/2.0",
    }

    submit = requests.post(
        f"{BASE_URL}/{slug}",
        headers=headers,
        json={"image_url": image_url, "prompt": prompt, "duration": duration, "sound": sound},
        timeout=timeout,
    )
    if submit.status_code >= 400:
        # Surface the server's explanation (403 body says WHY: scope/model/path).
        raise RuntimeError(f"Higgsfield submit {submit.status_code} @ /{slug}: {submit.text[:600]}")
    body = submit.json()
    status_url = body.get("status_url") or f"{BASE_URL}/requests/{body['request_id']}/status"

    video_url: Optional[str] = None
    for _ in range(max_polls):
        poll = requests.get(status_url, headers=headers, timeout=timeout)
        poll.raise_for_status()
        data = poll.json()
        status = data.get("status")
        if status == "completed":
            video_url = data["video"]["url"]
            break
        if status in ("failed", "nsfw", "cancelled"):
            raise RuntimeError(
                f"Higgsfield job ended status={status} "
                f"(credits refunded if nsfw/failed)"
            )
        time.sleep(poll_interval)

    if not video_url:
        raise RuntimeError(f"Higgsfield job did not complete within {max_polls} polls")

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mp4 = requests.get(video_url, timeout=timeout)
    mp4.raise_for_status()
    out_path.write_bytes(mp4.content)
    return out_path


class HiggsfieldGenerator:
    """Drop-in alternative to VeoGenerator for the 'higgsfield' backend (Kling i2v).

    Implements the subset of the VeoGenerator interface the worker calls:
    __init__, on_progress/on_error attrs, initialize_voice_profile, cancel,
    cleanup, generate_single_clip. No Gemini/OpenAI keys needed — the motion
    prompt is the clip's `veo_prompt_override` (the artifact's verbatim Veo
    prompt), and the start frame is animated by Kling via the Higgsfield API.
    """

    def __init__(self, config, job_id=None, api_keys=None, openai_key=None, **kwargs):
        self.config = config
        self.job_id = job_id
        self.api_keys = api_keys  # carried so worker code that reads generator.api_keys is safe
        self.openai_key = openai_key
        self.on_progress = None
        self.on_error = None
        # v761g — public `cancelled` (not `_cancelled`) to match the
        # VeoGenerator interface. worker.py polls `generator.cancelled` in
        # its clip loops (e.g. worker.py:3750); the private name left the
        # attribute missing and every higgsfield job crashed instantly with
        # AttributeError: 'HiggsfieldGenerator' object has no attribute 'cancelled'.
        self.cancelled = False
        self._overrides_by_dialogue_id = None  # lazy-loaded from job.dialogue_json

    # --- VeoGenerator-compatible lifecycle (mostly no-ops) ---
    def initialize_voice_profile(self, image_path):
        return None

    def cancel(self):
        self.cancelled = True

    def cleanup(self):
        return None

    def _emit_progress(self, clip_index, status, message, details=None):
        if self.on_progress:
            try:
                self.on_progress(clip_index, status, message, details or {})
            except Exception:
                pass

    def _load_overrides(self):
        """Build {dialogue_id: veo_prompt_override} from the job's dialogue_json."""
        if self._overrides_by_dialogue_id is not None:
            return self._overrides_by_dialogue_id
        mapping = {}
        try:
            import json
            from models import get_db, Job
            with get_db() as db:
                job = db.query(Job).filter(Job.id == self.job_id).first()
                if job and job.dialogue_json:
                    data = json.loads(job.dialogue_json)
                    for line in (data.get("lines") or []):
                        if isinstance(line, dict) and line.get("id") is not None:
                            mapping[line["id"]] = line.get("veo_prompt_override")
        except Exception as e:
            print(f"[higgsfield-kling] WARN could not load overrides: {e}", flush=True)
        self._overrides_by_dialogue_id = mapping
        return mapping

    def _motion_prompt(self, dialogue_id, dialogue_line):
        override = self._load_overrides().get(dialogue_id)
        if override and override.strip():
            return override.strip()
        # Fallback: the spoken line + a neutral motion instruction.
        return (dialogue_line or "").strip() or "Subtle natural motion, static locked-off camera."

    def generate_single_clip(
        self,
        start_frame,
        end_frame=None,
        dialogue_line="",
        dialogue_id=None,
        clip_index=0,
        output_dir=None,
        images_list=None,
        current_end_index=0,
        override_duration=None,
        **kwargs,
    ):
        """Animate start_frame with Kling. Returns the VeoGenerator result contract."""
        from pathlib import Path as _Path
        result = {
            "success": False,
            "output_path": None,
            "end_frame_used": None,
            "end_index": current_end_index,
            "error": None,
            "prompt_text": None,
        }
        try:
            from config import get_higgsfield_credentials_from_env, KLING_I2V_SLUG
            from backends.storage import get_storage
            from veo_generator import generate_output_filename
            from datetime import datetime
            import uuid

            creds = get_higgsfield_credentials_from_env()
            if not creds:
                result["error"] = "HF_KEY / HF_API_KEY+HF_API_SECRET not set on server"
                return result

            start_frame = _Path(start_frame)
            motion_prompt = self._motion_prompt(dialogue_id, dialogue_line)
            result["prompt_text"] = motion_prompt

            # Upload the start frame to R2 + presign a GET URL Higgsfield can fetch.
            storage = get_storage()
            tmp_key = f"higgsfield_tmp/{uuid.uuid4().hex}{start_frame.suffix or '.png'}"
            storage.upload_file(str(start_frame), tmp_key)
            try:
                # TTL > worst-case poll window (max_polls*poll_interval) + download.
                image_url = storage.get_presigned_url(tmp_key, expires_in=7200, method="get_object")

                duration = 5
                try:
                    duration = int(override_duration or getattr(self.config, "duration", 5))
                except (TypeError, ValueError):
                    duration = 5

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if getattr(self.config, "timestamp_names", True) else ""
                output_filename = generate_output_filename(dialogue_id, start_frame, None, timestamp)
                output_path = _Path(output_dir) / output_filename

                print(f"[higgsfield-kling] clip={clip_index} slug={KLING_I2V_SLUG} dur={duration} prompt_len={len(motion_prompt)}", flush=True)
                self._emit_progress(clip_index, "generating", f"Kling: {start_frame.name}", {"start": start_frame.name})

                animate_image(
                    image_url=image_url,
                    prompt=motion_prompt,
                    credentials=creds,
                    out_path=output_path,
                    slug=KLING_I2V_SLUG,
                    duration=duration,
                )
            finally:
                # Always remove the transient R2 upload — it exists only to give Kling a fetchable URL.
                try:
                    storage.delete(tmp_key)
                except Exception as _del_e:
                    print(f"[higgsfield-kling] WARN could not delete temp {tmp_key}: {_del_e}", flush=True)

            result["success"] = True
            result["output_path"] = output_path
            result["end_index"] = current_end_index
            self._emit_progress(clip_index, "completed", f"Saved: {output_filename}", {"output": output_filename})
            print(f"[higgsfield-kling] clip={clip_index} OK → {output_filename}", flush=True)
            return result
        except Exception as e:
            print(f"[higgsfield-kling] clip={clip_index} ERROR: {e}", flush=True)
            result["error"] = str(e)
            return result
