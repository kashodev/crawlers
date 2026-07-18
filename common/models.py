"""Metadata records persisted to Firestore."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DocState(str, Enum):
    DISCOVERED = "DISCOVERED"  # URL found, not yet fetched
    DOWNLOADED = "DOWNLOADED"  # stored successfully
    FAILED = "FAILED"  # exhausted retries
    SKIPPED = "SKIPPED"  # robots-disallowed, too large, or not a PDF


class JobStatus(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    # Ended early on a configured limit (miss streak / max documents) rather
    # than an error. Distinct from FAILED so alerting doesn't page on it.
    STOPPED_EARLY = "STOPPED_EARLY"


@dataclass
class DocumentRecord:
    """One PDF: where it came from, where it landed, and whether that worked."""

    doc_id: str
    site: str
    source_url: str
    # Page the PDF link was found on — the provenance trail for RAG citations.
    referrer_url: str | None = None
    state: str = DocState.DISCOVERED.value
    storage_uri: str | None = None
    storage_key: str | None = None
    content_hash: str | None = None
    content_length: int | None = None
    http_status: int | None = None
    attempts: int = 0
    error: str | None = None
    first_seen_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    downloaded_at: str | None = None
    last_job_id: str | None = None
    # Site-specific fields (bill number, year, title, sponsor, ...).
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class JobRecord:
    """One crawl run. Cloud Run task shards each write their own record."""

    job_id: str
    site: str
    status: str = JobStatus.RUNNING.value
    started_at: str = field(default_factory=_now)
    finished_at: str | None = None
    task_index: int = 0
    task_count: int = 1
    discovered: int = 0
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0
    bytes_stored: int = 0
    consecutive_misses: int = 0
    stop_reason: str | None = None
    error: str | None = None
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
