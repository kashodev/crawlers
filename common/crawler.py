"""Base crawler.

Subclasses implement `discover()`, yielding PdfCandidate objects newest-first.
Everything else — idempotency, download, storage, metadata, miss-streak
termination, job accounting, failure marking — lives here so the two site
modules stay small and behave identically.
"""

from __future__ import annotations

import logging
import os
import signal
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from common.config import SiteConfig
from common.http import FetchError, PoliteClient, RobotsDisallowed
from common.metadata import MetadataRepository, build_repository
from common.models import CheckpointRecord, DocState, DocumentRecord, JobRecord, JobStatus
from common.storage import Storage, build_storage
from utils.hashing import doc_id as make_doc_id
from utils.hashing import sha256_bytes, shard_index

log = logging.getLogger(__name__)

# How often a running job refreshes its heartbeat.
HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "30"))
# How long a RUNNING job may go without a heartbeat before it is presumed dead.
# Must stay well above the heartbeat interval so a slow task is never swept.
STALE_AFTER_SECONDS = float(os.environ.get("STALE_JOB_AFTER_SECONDS", "900"))


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


@dataclass
class ResumeState:
    """Where this run should pick up, and how that was determined."""

    cursor: dict[str, Any] | None = None
    frontier: dict[str, Any] | None = None
    sweep_complete: bool = False
    source: str = "none"  # "checkpoint" | "storage" | "none"
    last_status: str | None = None

    @property
    def is_resume(self) -> bool:
        """True when a previous run left the descending sweep unfinished."""
        return self.cursor is not None and not self.sweep_complete


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
        self._shutdown_reason: str | None = None
        self._since_checkpoint = 0
        self._last_heartbeat = time.monotonic()

        self.resume = self.load_resume_state()
        self.job.resumed_from = self.resume.cursor
        self.job.resume_source = self.resume.source
        self._checkpoint = CheckpointRecord(
            site=config.name,
            frontier=self.resume.frontier,
            cursor=self.resume.cursor,
            sweep_complete=self.resume.sweep_complete,
            last_job_id=self.job.job_id,
            last_status=JobStatus.RUNNING.value,
            task_index=self.task_index,
            task_count=self.task_count,
            derived_from=self.resume.source,
        )
        # Write the rebuilt position straight back, so a run that dies before
        # its first download still leaves a usable checkpoint behind.
        if self.resume.source not in ("none", ""):
            self.save_checkpoint()

    # --- to implement in site modules ------------------------------------

    @abstractmethod
    def discover(self) -> Iterator[PdfCandidate]:
        """Yield PDF candidates, most recent first."""

    def position_of(self, candidate: PdfCandidate) -> dict[str, Any] | None:
        """Position of a candidate in this site's ordering, for checkpointing.

        Returning None disables checkpointing for the site (it will still be
        idempotent, just not resumable).
        """
        return None

    def position_rank(self, position: dict[str, Any]) -> tuple:
        """Sortable rank for a position; higher means more recent."""
        return ()

    def position_from_key(self, key: str) -> dict[str, Any] | None:
        """Recover a position from a storage key, for checkpoint-less recovery."""
        return None

    def position_sort_key(self, position: dict[str, Any]) -> str | None:
        """Lexicographically sortable form of a position, stored on documents.

        Must sort identically to `position_rank`, since Firestore orders on the
        string while the crawler compares ranks in memory.
        """
        return None

    # --- resume -----------------------------------------------------------

    @property
    def checkpoint_id(self) -> str:
        """Checkpoint document id.

        Parallel tasks each own a disjoint shard and so keep separate
        checkpoints; a single-task job keeps the plain site-named document.
        """
        if self.task_count <= 1:
            return self.config.name
        return f"{self.config.name}--t{self.task_index}of{self.task_count}"

    def load_resume_state(self) -> ResumeState:
        """Determine where to resume, on every start.

        Firestore is always consulted first and is treated as the source of
        truth for what actually landed: two ordered, indexed queries return the
        last-written document (the cursor, since the sweep descends) and the
        highest-ranked one (the frontier). Each is then cross-checked against
        storage, scoped to that document's own prefix, so a metadata row whose
        object is missing does not send the crawl to the wrong place.

        The checkpoint is still read, but only to contribute `sweep_complete`
        and to advance the cursor past stretches the sweep probed without
        finding anything. That means a lost or stale checkpoint costs nothing:
        the position is rebuilt from Firestore and written back.
        """
        if os.environ.get("CRAWLER_NO_RESUME") == "1":
            log.info("Resume disabled; starting from the top of the range")
            return ResumeState()

        started = time.monotonic()
        checkpoint = self.repo.get_checkpoint(self.checkpoint_id) or {}
        cursor = self._verified_position(self.repo.latest_downloaded(self.config.name), "cursor")
        frontier = self._verified_position(
            self.repo.highest_downloaded(self.config.name), "frontier"
        )

        sources: list[str] = []
        if cursor or frontier:
            sources.append("firestore")

        # The checkpoint may sit below the last download because the sweep kept
        # probing through a gap. Lower is further along in a descending sweep,
        # so prefer it and avoid re-probing ground already covered.
        checkpoint_cursor = checkpoint.get("cursor")
        if checkpoint_cursor and (
            cursor is None
            or self.position_rank(checkpoint_cursor) < self.position_rank(cursor)
        ):
            cursor = checkpoint_cursor
            sources.append("checkpoint")

        checkpoint_frontier = checkpoint.get("frontier")
        if checkpoint_frontier and (
            frontier is None
            or self.position_rank(checkpoint_frontier) > self.position_rank(frontier)
        ):
            frontier = checkpoint_frontier
            if "checkpoint" not in sources:
                sources.append("checkpoint")

        elapsed = time.monotonic() - started

        if cursor is None and frontier is None:
            log.info(
                "No prior state; starting a fresh crawl",
                extra={"site": self.config.name, "lookupSeconds": round(elapsed, 3)},
            )
            return ResumeState()

        state = ResumeState(
            cursor=cursor,
            frontier=frontier,
            sweep_complete=bool(checkpoint.get("sweep_complete")),
            source="+".join(sources) or "none",
            last_status=checkpoint.get("last_status"),
        )
        log.info(
            "Resolved resume position",
            extra={
                "site": self.config.name,
                "checkpointId": self.checkpoint_id,
                "cursor": state.cursor,
                "frontier": state.frontier,
                "sweepComplete": state.sweep_complete,
                "source": state.source,
                "hadCheckpoint": bool(checkpoint),
                "lookupSeconds": round(elapsed, 3),
            },
        )
        return state

    def _verified_position(self, records: list[dict], label: str) -> dict[str, Any] | None:
        """First record from Firestore whose object is really in storage.

        Metadata can outlive its object — a deleted bucket, a partial restore, a
        write that landed before an upload failed. Walking a few candidates
        rather than trusting the first keeps one stale row from rewinding or
        over-advancing the resume position.
        """
        for record in records:
            key = record.get("storage_key")
            position = None
            if key:
                position = self.position_from_key(key)
            if position is None:
                continue
            try:
                if not self.storage.exists(key):
                    log.warning(
                        "Metadata references a missing object; trying the next candidate",
                        extra={"label": label, "storageKey": key},
                    )
                    continue
            except Exception as exc:
                # Storage unreachable: trust metadata rather than lose the run.
                log.warning(
                    "Could not verify object in storage; trusting metadata",
                    extra={"label": label, "storageKey": key, "error": str(exc)},
                )
            return position

        if records:
            log.warning(
                "No Firestore candidate could be confirmed in storage",
                extra={"label": label, "candidates": len(records)},
            )
        return None

    def recover_from_storage(self, sub_prefix: str = "") -> dict[str, Any] | None:
        """Highest position in storage under `sub_prefix`.

        A last resort for callers that know which slice to look in; unscoped
        use walks the whole bucket and should be avoided.
        """
        best: dict[str, Any] | None = None
        best_rank: tuple = ()
        try:
            for key in self.storage.list_keys(sub_prefix):
                position = self.position_from_key(key)
                if position is None:
                    continue
                rank = self.position_rank(position)
                if not best_rank or rank > best_rank:
                    best, best_rank = position, rank
        except Exception as exc:
            log.warning("Could not list storage", extra={"error": str(exc)})
            return None
        return best

    def note_progress(self, candidate: PdfCandidate) -> None:
        """Advance the checkpoint after a document is stored.

        The cursor only ever moves backwards (the sweep descends) and the
        frontier only ever moves forwards, so an out-of-order candidate — a new
        document found above the frontier — never rewinds resume position.
        """
        position = self.position_of(candidate)
        if position is None:
            return
        rank = self.position_rank(position)

        if self._checkpoint.frontier is None or rank > self.position_rank(
            self._checkpoint.frontier
        ):
            self._checkpoint.frontier = position
        if self._checkpoint.cursor is None or rank < self.position_rank(self._checkpoint.cursor):
            self._checkpoint.cursor = position

        # Persist periodically so a hard kill loses at most a few documents of
        # progress, not the whole run.
        self._since_checkpoint += 1
        if self._since_checkpoint >= 25:
            self.save_checkpoint()

    def save_checkpoint(self, status: str | None = None) -> None:
        self._checkpoint.last_job_id = self.job.job_id
        self._checkpoint.last_status = status or JobStatus.RUNNING.value
        self._checkpoint.updated_at = datetime.now(timezone.utc).isoformat()
        try:
            self.repo.save_checkpoint(self.checkpoint_id, self._checkpoint)
            self._since_checkpoint = 0
        except Exception as exc:
            # A checkpoint failure must not kill a working crawl; worst case we
            # resume from an older position next time.
            log.error("Failed to save checkpoint", extra={"error": str(exc)})

    def mark_sweep_complete(self) -> None:
        """Called by a crawler once its descending sweep covers the full range."""
        log.info("Sweep complete; future runs only check for new documents")
        self._checkpoint.sweep_complete = True
        self.save_checkpoint()

    # --- liveness -----------------------------------------------------------

    def _maybe_heartbeat(self) -> None:
        """Refresh the job's heartbeat, at most once per interval.

        Time-bounded rather than per-document so the write rate stays flat
        whether the crawl is downloading large PDFs slowly or skipping
        thousands of already-stored ones quickly.
        """
        now = time.monotonic()
        if now - self._last_heartbeat < HEARTBEAT_INTERVAL_SECONDS:
            return
        self._last_heartbeat = now
        self.job.last_heartbeat_at = datetime.now(timezone.utc).isoformat()
        try:
            self.repo.update_job(
                self.job.job_id,
                {
                    "last_heartbeat_at": self.job.last_heartbeat_at,
                    "discovered": self.job.discovered,
                    "downloaded": self.job.downloaded,
                    "skipped": self.job.skipped,
                    "failed": self.job.failed,
                },
            )
        except Exception as exc:
            # Losing a heartbeat is not worth failing a crawl over; the worst
            # case is this job being swept as STALE later.
            log.warning("Heartbeat write failed", extra={"error": str(exc)})

    def sweep_stale_jobs(self) -> int:
        """Mark abandoned RUNNING jobs for this site as STALE.

        A job killed without a usable grace period — a console cancellation
        that outran SIGTERM, SIGKILL, OOM, preemption — never gets to write a
        terminal status, so it sits at RUNNING forever. Any RUNNING job whose
        heartbeat is older than the threshold is therefore presumed dead.

        The threshold is far larger than the heartbeat interval, so a merely
        slow task is never mistaken for a dead one.
        """
        threshold = timedelta(seconds=STALE_AFTER_SECONDS)
        now = datetime.now(timezone.utc)
        swept = 0

        try:
            candidates = self.repo.find_running_jobs()
        except Exception as exc:
            log.warning("Stale-job sweep skipped", extra={"error": str(exc)})
            return 0

        for record in candidates:
            job_id = record.get("job_id")
            if not job_id or job_id == self.job.job_id:
                continue
            if record.get("site") != self.config.name:
                continue

            # started_at covers a job that died before its first heartbeat.
            stamp = record.get("last_heartbeat_at") or record.get("started_at")
            try:
                last_seen = datetime.fromisoformat(stamp)
            except (TypeError, ValueError):
                log.debug("Job has no usable timestamp; leaving it", extra={"jobId": job_id})
                continue

            age = now - last_seen
            if age < threshold:
                continue

            self.repo.update_job(
                job_id,
                {
                    "status": JobStatus.STALE.value,
                    "finished_at": now.isoformat(),
                    "stop_reason": (
                        f"no heartbeat for {int(age.total_seconds())}s; "
                        "presumed killed (cancellation, preemption, or OOM)"
                    ),
                    "marked_stale_at": now.isoformat(),
                    "marked_stale_by": self.job.job_id,
                },
            )
            swept += 1
            log.warning(
                "Marked abandoned job STALE",
                extra={
                    "jobId": job_id,
                    "ageSeconds": int(age.total_seconds()),
                    "lastSeen": stamp,
                },
            )

        if swept:
            log.info("Stale-job sweep complete", extra={"marked": swept})
        return swept

    # --- graceful shutdown -------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Turn SIGTERM/SIGINT into a clean stop.

        Cloud Run sends SIGTERM before killing a task that hits its timeout or
        gets preempted. Catching it lets the run flush metadata and write a
        checkpoint, so the next execution resumes instead of starting over.
        """

        def handle(signum, _frame):
            name = signal.Signals(signum).name
            if self._shutdown_reason is None:
                self._shutdown_reason = f"received {name}"
                log.warning("Shutdown signal received; stopping after current document",
                            extra={"signal": name})

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, handle)
            except ValueError:
                # Not the main thread (e.g. under a test runner); skip.
                log.debug("Could not install handler for %s", sig)

    def _check_shutdown(self) -> None:
        if self._shutdown_reason:
            raise StopCrawl(self._shutdown_reason)

    # --- shared machinery -------------------------------------------------

    def owns(self, shard_key: str | int) -> bool:
        """Whether this task shard handles `shard_key`.

        Called once per work unit, during discovery only. Sharding a second
        time downstream would multiply the filters and drop most of the work,
        so `process()` deliberately does not re-check ownership.
        """
        if self.task_count <= 1:
            return True
        return shard_index(shard_key, self.task_count) == self.task_index

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

        if self._should_skip(doc_id, candidate):
            log.debug("Already downloaded; skipping", extra={"url": candidate.url})
            self.job.skipped += 1
            return DocState.SKIPPED.value

        existing = self.repo.get_document(doc_id) or {}
        position = self.position_of(candidate)
        record = DocumentRecord(
            doc_id=doc_id,
            site=self.config.name,
            source_url=candidate.url,
            referrer_url=candidate.referrer_url,
            sort_key=self.position_sort_key(position) if position else None,
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
            self.note_progress(candidate)
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
        self._install_signal_handlers()
        log.info(
            "Crawl starting",
            extra={
                "jobId": self.job.job_id,
                "site": self.config.name,
                "taskIndex": self.task_index,
                "taskCount": self.task_count,
                "seeds": self.config.seeds,
                "resumeSource": self.resume.source,
                "resumeCursor": self.resume.cursor,
                "frontier": self.resume.frontier,
                "isResume": self.resume.is_resume,
            },
        )
        self.repo.save_job(self.job)

        # Only one task needs to sweep; the writes are identical either way,
        # but there is no reason for every shard to repeat the scan.
        if self.task_index == 0:
            self.sweep_stale_jobs()

        try:
            for candidate in self.discover():
                self._check_shutdown()
                self._maybe_heartbeat()
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
            self.job.status = JobStatus.STOPPED_EARLY.value
            self.job.stop_reason = "interrupted"
            log.warning("Crawl interrupted; progress checkpointed")

        except Exception as exc:
            # Mark the job FAILED so a partial run is never mistaken for a
            # complete one; the container also exits non-zero (see main.py) so
            # Cloud Run surfaces and retries the task.
            self.job.status = JobStatus.FAILED.value
            self.job.error = f"{type(exc).__name__}: {exc}"
            log.exception("Crawl failed", extra={"jobId": self.job.job_id})

        finally:
            self.job.finished_at = datetime.now(timezone.utc).isoformat()
            # Order matters: flush documents first, then checkpoint. A
            # checkpoint that outran its documents would skip them on resume.
            try:
                self.repo.flush()
            finally:
                self.save_checkpoint(self.job.status)
                self.repo.save_job(self.job)
                self.client.close()

        log.info(
            "Crawl finished",
            extra={
                "jobId": self.job.job_id,
                "status": self.job.status,
                "discovered": self.job.discovered,
                "downloaded": self.job.downloaded,
                "newDocuments": self.job.new_documents,
                "skipped": self.job.skipped,
                "failed": self.job.failed,
                "bytesStored": self.job.bytes_stored,
                "stopReason": self.job.stop_reason,
                "resumeCursor": self._checkpoint.cursor,
                "frontier": self._checkpoint.frontier,
                "sweepComplete": self._checkpoint.sweep_complete,
            },
        )
        return self.job
