"""Structured logging.

Cloud Logging picks up JSON on stdout and parses `severity`/`message`
automatically, so the same logger works locally (plain text) and on Cloud Run
(structured) with no call-site changes.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Cloud Run injects these; they let us correlate logs with a specific task shard.
TASK_INDEX = os.environ.get("CLOUD_RUN_TASK_INDEX")
TASK_COUNT = os.environ.get("CLOUD_RUN_TASK_COUNT")

_LEVEL_TO_SEVERITY = {
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "WARNING": "WARNING",
    "ERROR": "ERROR",
    "CRITICAL": "CRITICAL",
}

_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "asctime",
    "message",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "severity": _LEVEL_TO_SEVERITY.get(record.levelname, "DEFAULT"),
            "message": record.getMessage(),
            "logger": record.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if TASK_INDEX is not None:
            payload["taskIndex"] = int(TASK_INDEX)
            payload["taskCount"] = int(TASK_COUNT or 1)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Anything passed via logger.info("...", extra={...}) rides along as a
        # structured field, which is what makes the logs queryable in GCP.
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        return json.dumps(payload, default=str)


def configure_logging(level: str | None = None, structured: bool | None = None) -> None:
    """Install the root handler. Structured by default when running on GCP."""
    if structured is None:
        structured = bool(os.environ.get("K_SERVICE") or TASK_INDEX is not None)
    resolved = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter()
        if structured
        else logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved)

    # These are chatty at DEBUG and drown out crawl progress.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
