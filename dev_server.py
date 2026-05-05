"""Local-dev launcher.

Wraps uvicorn with the httptools HTTP impl. The default `h11` backend
triggers a sandbox-blocked filesystem path-importer-cache lookup on this
machine; httptools (C-based) avoids it. Existing because the IDE preview
harness auto-rewrites launch.json's runtimeArgs back to `--http h11`,
which we work around by routing through this script.
"""
from __future__ import annotations

import sys
import uvicorn


def main() -> None:
    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=8765,
        log_level="warning",
        http="httptools",
        loop="asyncio",
        # Hot-reload Python edits — saves a kill/restart cycle on every
        # backend tweak. JS/CSS edits already hot-reload via the
        # mtime-hash cache-buster + Cmd+Shift+R.
        reload=True,
        reload_dirs=[".", "assets"],
        reload_includes=["*.py", "*.json"],
    )


if __name__ == "__main__":
    sys.exit(main() or 0)
