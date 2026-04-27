---
title: Chokepoint Intel
emoji: 🛢️
colorFrom: red
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
license: mit
short_description: Red Sea / Hormuz energy-security intelligence dashboard
---

# Chokepoint Intel

Red Sea / Strait of Hormuz energy-security intelligence dashboard.
Single-page HTML front-end + FastAPI backend.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env         # fill in EIA / FRED / ACLED keys (optional)
python app.py                # → http://localhost:8000
```

## Deploy to Hugging Face Spaces (free 16 GB RAM)

The repo includes a `Dockerfile` so a Hugging Face Space can build the
dashboard directly from this GitHub remote.

1. Create a new Space at <https://huggingface.co/new-space>
   - **SDK**: `Docker`
   - **Hardware**: `CPU basic` (free, 16 GB RAM)
2. In the Space's **Settings → Repository secrets**, add:
   - `ACLED_USERNAME`, `ACLED_PASSWORD`
   - `EIA_API_KEY`
   - `FRED_API_KEY`
3. Either link the Space to this GitHub repo (Settings → "Linked Models &
   Datasets") or push the code to the Space's git remote:
   ```bash
   git remote add hf https://huggingface.co/spaces/<user>/<space-name>
   git push hf main
   ```
4. The Space rebuilds the Docker image automatically on push.

**Read [HANDOFF.md](./HANDOFF.md) for the full developer guide**: architecture, endpoints, deployment notes, extension points, and known gaps.
