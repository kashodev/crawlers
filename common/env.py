"""Environment loading.

Reads .env once at startup so local runs get the same configuration surface as
Cloud Run, where the job spec supplies these as real environment variables.
Real environment variables always win over .env — that is what keeps the
deployed job authoritative over any .env baked into an image by accident.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

_loaded = False


def load_env(path: str | Path | None = None) -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True

    try:
        from dotenv import load_dotenv
    except ImportError:
        log.debug("python-dotenv not installed; using process environment only")
        return

    env_path = Path(path) if path else Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        # override=False: a value already in the environment beats the file.
        load_dotenv(env_path, override=False)
        log.debug("Loaded .env", extra={"path": str(env_path)})

    # Blank assignments in .env arrive as empty strings, which would otherwise
    # read as "configured" and, say, switch storage to GCS with no bucket name.
    for key, value in list(os.environ.items()):
        if value == "":
            del os.environ[key]
