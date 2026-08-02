"""Build the unpacked-test Chrome extension ZIP without server dependencies."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path


def build_extension_zip(extension_dir: Path) -> bytes:
    if not extension_dir.is_dir():
        raise FileNotFoundError("ChatGPT extension bundle is not present")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(extension_dir.rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                archive.write(path, path.relative_to(extension_dir).as_posix())
    return buffer.getvalue()
