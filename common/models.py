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
    # Lexicographically sortable position, e.g. "2026-000713". Indexed so
    # resume can find the newest/last document with a single ordered query
    # instead of listing storage.
    sort_key: str | None = None
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
class CheckpointRecord:
    """Resumable position for a site, updated as documents are stored.

    Two positions are tracked because they answer different questions:

      frontier — the newest document ever downloaded. Anything above it is new
                 material published since the last run.
      cursor   — the oldest position the descending sweep has reached. A run
                 that dies mid-sweep resumes just below this instead of
                 re-probing the whole range from the top.

    `sweep_complete` records whether the descending sweep ever reached the end
    of the configured range. Until it does, every run resumes before it looks
    for new documents.
    """

    site: str
    frontier: dict[str, Any] | None = None
    cursor: dict[str, Any] | None = None
    sweep_complete: bool = False
    last_job_id: str | None = None
    last_status: str | None = None
    updated_at: str = field(default_factory=_now)
    # Parallel tasks each own a shard and keep their own checkpoint, so they
    # never overwrite one another's position.
    task_index: int = 0
    task_count: int = 1
    # How this checkpoint was last derived, for debugging a bad resume.
    derived_from: str | None = None

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
    # Where this run picked up from, and how that position was recovered
    # ("checkpoint", "storage", or "none" for a fresh crawl).
    resumed_from: dict[str, Any] | None = None
    resume_source: str = "none"
    # Documents found by the new-material pass rather than the resume sweep.
    new_documents: int = 0
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
