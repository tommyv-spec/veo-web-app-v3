# Veo Web App - Production Dockerfile
# Optimized for Render deployment

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Install system dependencies including ffmpeg, espeak-ng (for aeneas
# forced-alignment fallback), and build tools (aeneas pip install needs
# to compile a C extension).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgl1 \
    curl \
    espeak-ng \
    libespeak-ng-dev \
    build-essential \
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
# v773.10.10 — pre-install numpy + setuptools BEFORE the main requirements
# pass. aeneas' setup.py imports numpy at build time, and pip's resolver
# does NOT guarantee install order — so a direct `pip install -r requirements.txt`
# fails with "[ERRO] You must install numpy before installing aeneas".
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir "numpy>=1.24.0" && \
    pip install --no-cache-dir -r requirements.txt

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
