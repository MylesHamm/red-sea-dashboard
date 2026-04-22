# Chokepoint Intel

Red Sea / Strait of Hormuz energy-security intelligence dashboard.
Single-page HTML front-end + FastAPI backend.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env         # fill in EIA / FRED / ACLED keys (optional)
python app.py                # → http://localhost:8000
```

**Read [HANDOFF.md](./HANDOFF.md) for the full developer guide**: architecture, endpoints, deployment (Render blueprint included), extension points, and known gaps.
