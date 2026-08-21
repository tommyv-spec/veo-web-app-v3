# Veo Web App - Production Dockerfile
# Optimized for Render deployment

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# v872 — glibc allocator settings. These are set HERE, not only in render.yaml,
# because they must apply whether or not the blueprint is synced.
#
# Why: the 2026-07-28 export OOM traced to RSS that climbed from 173MB at boot
# to ~1.7GB during one export and never came back down, on a 2GB cgroup. Two
# glibc defaults cause exactly that:
#   * one malloc arena PER THREAD (up to 8x cores, and this app runs a large
#     anyio thread pool) — each arena keeps its own freed pages forever
#   * a DYNAMIC mmap threshold that grows to 32MB, so the big short-lived
#     buffers this app allocates (audio blobs, uploaded mp4s, model weights)
#     land on the heap, where free() cannot return them to the kernel
# ARENA_MAX=2 caps arena sprawl. A fixed 128KB mmap threshold sends every large
# allocation to mmap, where free() unmaps it immediately. TRIM_THRESHOLD keeps
# the main heap from sitting on a large idle top.
ENV MALLOC_ARENA_MAX=2
ENV MALLOC_MMAP_THRESHOLD_=131072
ENV MALLOC_TRIM_THRESHOLD_=131072
ENV MALLOC_TOP_PAD_=131072

# Install system dependencies including ffmpeg.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgl1 \
    libegl1 \
    libgles2 \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Verify ffmpeg installation
RUN ffmpeg -version && ffprobe -version

# Set ffmpeg environment variables
ENV FFMPEG_BIN=/usr/bin/ffmpeg
ENV FFPROBE_BIN=/usr/bin/ffprobe

# Create app directory
WORKDIR /app

# Create directories for data persistence
# Note: On Render, you'll need a persistent disk mounted at /data
RUN mkdir -p /app/data /app/uploads /app/outputs /app/static

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# v938.6 — the caption renderer (pycaps) draws through a real browser engine.
# That is what gives the captions their TikTok/CapCut look: rounded highlight,
# pop-in, real typography, and the dozen ready presets. Without Chromium the
# server falls back to a plainer ffmpeg/libass renderer that visibly is not
# the same thing. Installed while still root — the USER switch is below.
# --with-deps pulls the system libraries headless Chromium needs on slim.
# PLAYWRIGHT_BROWSERS_PATH is set BEFORE the install on purpose: the default
# lands in /root/.cache, which the non-root appuser below cannot read, so the
# browser would be present and unusable at runtime.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
# playwright is an OPTIONAL extra of pycaps, so it is named explicitly here —
# without it `python -m playwright install` fails with "No module named
# playwright" and the whole build stops.
#
# opencv-python is pinned <5 because pycaps depends on it and pip happily
# resolved 5.0.0.93, which SHADOWS the opencv-python-headless<5 in
# requirements.txt. OpenCV 5 has no cv2.CascadeClassifier, so that would have
# silently re-broken the face detection the caption placement needs — the same
# failure this project already hit once. Non-headless is fine here: libgl1,
# libsm6 and libxext6 are installed above.
RUN pip install --no-cache-dir \
        "pycaps @ git+https://github.com/francozanardi/pycaps" \
        playwright \
        "opencv-python<5" && \
    python -m playwright install --with-deps chromium && \
    chmod -R a+rX /ms-playwright && \
    python -c "import cv2, sys; assert hasattr(cv2,'CascadeClassifier'), 'cv2 lost CascadeClassifier'; print('cv2', cv2.__version__, 'ok')" && \
    python -c "import skia; print('skia ok')" && \
    rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Start command with Gunicorn + Uvicorn workers
# Using 1 async worker — all endpoints are async def, heavy work runs in
# asyncio.to_thread (FFmpeg, Whisper). Event loop stays responsive for
# hundreds of concurrent API requests. 1 worker avoids duplicate memory
# (Whisper model ~300MB) which causes OOM on 512MB instances.
# v508: max_requests raised from 500 → 50000 (jitter 50 → 5000).
# At this app's sustained ~2.4 req/sec rate, 500 caused the worker to
# restart every ~3.5 minutes, which interrupted background tasks
# (job setup, assembly), dropped in-flight requests, and replayed
# all migrations at every restart (wasted DB work, ~17 idempotent
# ALTER TABLE checks per cycle).
# 50000 = ~6 hours between recycles at current traffic. If the
# memory-leak protection is ever needed it still kicks in eventually,
# but not constantly. Most healthy apps disable max_requests entirely.
CMD ["gunicorn", "main:app", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--workers", "1", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "300", \
     "--keep-alive", "5", \
     "--max-requests", "50000", \
     "--max-requests-jitter", "5000", \
     "--access-logfile", "/dev/null", \
     "--error-logfile", "-"]
