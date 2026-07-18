"""Base crawler.

Subclasses implement `discover()`, yielding PdfCandidate objects newest-first.
Everything else — idempotency, download, storage, metadata, miss-streak
termination, job accounting, failure marking — lives here so the two site
modules stay small and behave identically.
"""

from __future__ import annotations

import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

from common.config import SiteConfig
from common.http import FetchError, PoliteClient, RobotsDisallowed
from common.metadata import MetadataRepository, build_repository
from common.models import DocState, DocumentRecord, JobRecord, JobStatus
from common.storage import Storage, build_storage
from utils.hashing import doc_id as make_doc_id
from utils.hashing import sha256_bytes

log = logging.getLogger(__name__)


@dataclass
class PdfCandidate:
    """A PDF the discovery pass believes exists."""

    url: str
    storage_key: str
    referrer_url: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)


class StopCrawl(Exception):
    """Raised internally to unwind discovery when a stop condition is hit."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class BaseCrawler(ABC):
    def __init__(
        self,
        config: SiteConfig,
        client: PoliteClient | None = None,
        storage: Storage | None = None,
        repository: MetadataRepository | None = None,
        job_id: str | None = None,
    ):
        self.config = config
        self.client = client or PoliteClient(config.politeness, config.retry)
        self.storage = storage or build_storage(config.storage)
        self.repo = repository or build_repository(config.metadata)

        # Cloud Run Jobs shards via these env vars; locally it's a single task.
        self.task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
        self.task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base_id = job_id or f"{config.name}-{stamp}-{uuid.uuid4().hex[:8]}"
        self.job = JobRecord(
            job_id=base_id if self.task_count == 1 else f"{base_id}-t{self.task_index}",
            site=config.name,
            task_index=self.task_index,
            task_count=self.task_count,
            config_snapshot={
                "seeds": config.seeds,
                "minDelaySeconds": config.politeness.min_delay_seconds,
                "consecutiveMissLimit": config.limits.consecutive_miss_limit,
                "maxDocuments": config.limits.max_documents,
                "options": config.options,
            },
        )
        self._consecutive_misses = 0

    # --- to implement in site modules ------------------------------------

    @abstractmethod
    def discover(self) -> Iterator[PdfCandidate]:
        """Yield PDF candidates, most recent first."""

    # --- shared machinery -------------------------------------------------

    def owns(self, shard_key: str | int) -> bool:
        """Whether this task shard handles `shard_key`.

        Discovery runs in every task, but each one downloads a disjoint slice.
        Keeps parallel tasks from duplicating work or racing on the same object.
        """
        if self.task_count <= 1:
            return True
        return hash(str(shard_key)) % self.task_count == self.task_index

    def record_miss(self) -> None:
        """Note a probe that yielded no PDF, and stop the crawl if the streak is long enough."""
        self._consecutive_misses += 1
        self.job.consecutive_misses = self._consecutive_misses
        limit = self.config.limits.consecutive_miss_limit
        if limit and self._consecutive_misses >= limit:
            raise StopCrawl(f"{self._consecutive_misses} consecutive probes yielded no PDF")
        if self._consecutive_misses % 100 == 0:
            log.info("Miss streak growing", extra={"consecutiveMisses": self._consecutive_misses})

    def record_hit(self) -> None:
        self._consecutive_misses = 0
        self.job.consecutive_misses = 0

    def _should_skip(self, doc_id: str, candidate: PdfCandidate) -> bool:
        """Idempotency check: has this document already been stored successfully?"""
        existing = self.repo.get_document(doc_id)
        if existing and existing.get("state") == DocState.DOWNLOADED.value:
            key = existing.get("storage_key") or candidate.storage_key
            # Trust metadata only if the object is actually still in storage;
            # otherwise a deleted bucket object would never be re-fetched.
            if self.storage.exists(key):
                return True
            log.info(
                "Metadata says downloaded but object is missing; re-fetching",
                extra={"docId": doc_id, "url": candidate.url},
            )
        return False

    def process(self, candidate: PdfCandidate) -> str:
        """Download and store one candidate. Returns the resulting DocState."""
        doc_id = make_doc_id(candidate.url)

        if not self.owns(candidate.storage_key):
            return "NOT_OWNED"

        if self._should_skip(doc_id, candidate):
            log.debug("Already downloaded; skipping", extra={"url": candidate.url})
            self.job.skipped += 1
            return DocState.SKIPPED.value

        existing = self.repo.get_document(doc_id) or {}
        record = DocumentRecord(
            doc_id=doc_id,
            site=self.config.name,
            source_url=candidate.url,
            referrer_url=candidate.referrer_url,
            first_seen_at=existing.get("first_seen_at") or datetime.now(timezone.utc).isoformat(),
            attempts=int(existing.get("attempts") or 0) + 1,
            last_job_id=self.job.job_id,
            attributes=candidate.attributes,
        )

        try:
            resp = self.client.download(candidate.url, self.config.limits.max_pdf_bytes)
            record.http_status = resp.status_code

            # A 200 that returns an HTML error page is a miss, not a document.
            if resp.content_type not in ("application/pdf", "application/octet-stream"):
                if not resp.content.startswith(b"%PDF"):
                    raise FetchError(
                        f"expected PDF, got {resp.content_type or 'unknown'}", resp.status_code
                    )

            record.content_hash = sha256_bytes(resp.content)
            record.content_length = len(resp.content)
            record.storage_key = candidate.storage_key
            record.storage_uri = self.storage.write(candidate.storage_key, resp.content)
            record.state = DocState.DOWNLOADED.value
            record.downloaded_at = datetime.now(timezone.utc).isoformat()

            self.job.downloaded += 1
            self.job.bytes_stored += record.content_length
            log.info(
                "Downloaded PDF",
                extra={
                    "url": candidate.url,
                    "uri": record.storage_uri,
                    "bytes": record.content_length,
                    "attempt": record.attempts,
                },
            )

        except RobotsDisallowed as exc:
            record.state = DocState.SKIPPED.value
            record.error = str(exc)
            self.job.skipped += 1
            log.warning("Skipped (robots.txt)", extra={"url": candidate.url})

        except (FetchError, Exception) as exc:
            record.state = DocState.FAILED.value
            record.error = f"{type(exc).__name__}: {exc}"
            record.http_status = record.http_status or getattr(exc, "status_code", None)
            self.job.failed += 1
            log.error(
                "Failed to download PDF",
                extra={
                    "url": candidate.url,
                    "error": record.error,
                    "attempt": record.attempts,
                },
            )

        record.updated_at = datetime.now(timezone.utc).isoformat()
        self.repo.stage_document(record)
        return record.state

    def run(self) -> JobRecord:
        """Execute the crawl and persist the job record."""
        log.info(
            "Crawl starting",
            extra={
                "jobId": self.job.job_id,
                "site": self.config.name,
                "taskIndex": self.task_index,
                "taskCount": self.task_count,
                "seeds": self.config.seeds,
            },
        )
        self.repo.save_job(self.job)

        try:
            for candidate in self.discover():
                self.job.discovered += 1
                self.process(candidate)

                max_docs = self.config.limits.max_documents
                if max_docs and self.job.downloaded >= max_docs:
                    raise StopCrawl(f"reached max_documents={max_docs}")

            self.job.status = JobStatus.COMPLETED.value

        except StopCrawl as stop:
            # An intentional early exit, not a failure.
            self.job.status = JobStatus.STOPPED_EARLY.value
            self.job.stop_reason = stop.reason
            log.info("Crawl stopped early", extra={"reason": stop.reason})

        except KeyboardInterrupt:
            self.job.status = JobStatus.FAILED.value
            self.job.error = "interrupted"
            log.warning("Crawl interrupted")
            raise

        except Exception as exc:
            # Mark the job FAILED so a partial run is never mistaken for a
            # complete one; the container also exits non-zero (see main.py) so
            # Cloud Run surfaces and retries the task.
            self.job.status = JobStatus.FAILED.value
            self.job.error = f"{type(exc).__name__}: {exc}"
            log.exception("Crawl failed", extra={"jobId": self.job.job_id})

        finally:
            self.job.finished_at = datetime.now(timezone.utc).isoformat()
            try:
                self.repo.flush()
            finally:
                self.repo.save_job(self.job)
                self.client.close()

        log.info(
            "Crawl finished",
            extra={
                "jobId": self.job.job_id,
                "status": self.job.status,
                "discovered": self.job.discovered,
                "downloaded": self.job.downloaded,
                "skipped": self.job.skipped,
                "failed": self.job.failed,
                "bytesStored": self.job.bytes_stored,
                "stopReason": self.job.stop_reason,
            },
        )
        return self.job
