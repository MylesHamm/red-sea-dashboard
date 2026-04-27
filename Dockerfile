# ─── Hugging Face Spaces deployment ────────────────────────────────────────
# HF Spaces expects a Dockerfile in the repo root and listens on port 7860.
# We pin Python 3.11 (matches Render setup), install deps, and run uvicorn
# with --loop asyncio to avoid the uvloop dep on macOS-style sandboxes.
#
# Secrets (ACLED creds, EIA / FRED keys) are injected as env vars via the
# Spaces UI → Settings → Repository secrets, NOT baked into the image.
# config.py reads them via os.environ so no code change is needed.
#
# Persistent storage: HF Spaces writes the working directory each restart,
# so the .cache/ dir is repopulated on the first request set after boot
# via the existing preload thread in app.py. No `/data` mount required.

FROM python:3.11-slim

# System packages: tini for clean signal handling so HF can stop the
# container without orphaning the uvicorn workers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
    && rm -rf /var/lib/apt/lists/*

# HF Spaces convention: app runs as a non-root user inside /home/user/app
RUN useradd -m -u 1000 user
USER user
WORKDIR /home/user/app

# Layer 1: install deps. Cached unless requirements.txt changes — speeds
# up subsequent rebuilds from minutes to seconds.
COPY --chown=user:user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Layer 2: copy the app. Excluded from the build context via .dockerignore
# so .env / .cache / .git don't bloat the image.
COPY --chown=user:user . .

ENV PATH="/home/user/.local/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
# HF Spaces routes external traffic to whatever port the container exposes;
# 7860 is the convention but uvicorn picks up $PORT if Spaces overrides it.
ENV PORT=7860
EXPOSE 7860

# tini → uvicorn so SIGTERM from HF actually shuts the worker cleanly
ENTRYPOINT ["tini", "--"]
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860} --loop asyncio --log-level info"]
