"""Metadata repositories.

Firestore in production; a JSON-file store locally so the crawler is runnable
without any GCP credentials. Document writes are batched (Firestore caps a
batch at 500) because per-PDF writes are the one metering line that actually
costs money on a 100k-document crawl.
"""

from __future__ import annotations

import json
import logging
import threading
from abc import ABC, abstractmethod
from pathlib import Path

from common.config import MetadataConfig
from common.models import CheckpointRecord, DocumentRecord, JobRecord

log = logging.getLogger(__name__)


class MetadataRepository(ABC):
    @abstractmethod
    def get_document(self, doc_id: str) -> dict | None: ...

    @abstractmethod
    def stage_document(self, record: DocumentRecord) -> None:
        """Queue a document write; flushed in batches."""

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def save_job(self, job: JobRecord) -> None: ...

    @abstractmethod
    def find_running_jobs(self, limit: int = 200) -> list[dict]:
        """Jobs still marked RUNNING, across all sites.

        Filtered on status alone — a single-field index every Firestore
        database has by default — so the stale sweep needs no composite index.
        Callers narrow by site in memory; the set of stuck jobs is small.
        """

    @abstractmethod
    def update_job(self, job_id: str, fields: dict) -> None:
        """Merge partial fields into an existing job record."""

    @abstractmethod
    def get_checkpoint(self, checkpoint_id: str) -> dict | None: ...

    @abstractmethod
    def save_checkpoint(self, checkpoint_id: str, checkpoint: CheckpointRecord) -> None: ...

    @abstractmethod
    def iter_documents(self, start_after: str | None = None, page_size: int = 500):
        """Yield every document in id order, paginated.

        Ordered by id so a migration can record the last id it handled and
        resume from there instead of restarting.
        """

    @abstractmethod
    def update_documents(self, updates: list[tuple[str, dict]]) -> None:
        """Merge partial field updates into existing documents, in batches."""

    @abstractmethod
    def replace_documents(self, updates: list[tuple[str, dict]]) -> None:
        """Replace top-level fields wholesale, in batches.

        Unlike update_documents, a map-valued field is set to exactly the given
        map — keys absent from it are removed. Use this to rewrite a nested map
        like `attributes` when a migration needs to drop a legacy key, not just
        add one. (update_documents deep-merges on Firestore, so it can add but
        never remove a nested key.)
        """

    @abstractmethod
    def latest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        """Most recently downloaded documents, newest write first.

        In a descending sweep the last write is the furthest the crawl got, so
        this is the resume cursor.
        """

    @abstractmethod
    def highest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        """Downloaded documents with the highest sort_key — the frontier."""

    def close(self) -> None:
        self.flush()


class LocalMetadataRepository(MetadataRepository):
    """JSON-backed store for local development."""

    def __init__(self, root: str, documents_collection: str, jobs_collection: str):
        self.docs_dir = Path(root) / documents_collection
        self.jobs_dir = Path(root) / jobs_collection
        self.checkpoints_dir = Path(root) / "checkpoints"
        for directory in (self.docs_dir, self.jobs_dir, self.checkpoints_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._pending: dict[str, DocumentRecord] = {}
        self._lock = threading.Lock()

    def get_document(self, doc_id: str) -> dict | None:
        with self._lock:
            if doc_id in self._pending:
                return self._pending[doc_id].to_dict()
        path = self.docs_dir / f"{doc_id}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("Corrupt local metadata; treating as absent", extra={"docId": doc_id})
            return None

    def stage_document(self, record: DocumentRecord) -> None:
        with self._lock:
            self._pending[record.doc_id] = record

    def flush(self) -> None:
        with self._lock:
            pending, self._pending = self._pending, {}
        for doc_id, record in pending.items():
            (self.docs_dir / f"{doc_id}.json").write_text(
                json.dumps(record.to_dict(), indent=2), encoding="utf-8"
            )
        if pending:
            log.debug("Flushed local metadata", extra={"count": len(pending)})

    def save_job(self, job: JobRecord) -> None:
        (self.jobs_dir / f"{job.job_id}.json").write_text(
            json.dumps(job.to_dict(), indent=2), encoding="utf-8"
        )

    def find_running_jobs(self, limit: int = 200) -> list[dict]:
        running = []
        for path in self.jobs_dir.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if record.get("status") == "RUNNING":
                running.append(record)
                if len(running) >= limit:
                    break
        return running

    def update_job(self, job_id: str, fields: dict) -> None:
        path = self.jobs_dir / f"{job_id}.json"
        if not path.exists():
            return
        record = json.loads(path.read_text(encoding="utf-8"))
        record.update(fields)
        path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    def get_checkpoint(self, checkpoint_id: str) -> dict | None:
        path = self.checkpoints_dir / f"{checkpoint_id}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("Corrupt checkpoint; ignoring", extra={"checkpointId": checkpoint_id})
            return None

    def save_checkpoint(self, checkpoint_id: str, checkpoint: CheckpointRecord) -> None:
        (self.checkpoints_dir / f"{checkpoint_id}.json").write_text(
            json.dumps(checkpoint.to_dict(), indent=2), encoding="utf-8"
        )

    def iter_documents(self, start_after: str | None = None, page_size: int = 500):
        for path in sorted(self.docs_dir.glob("*.json"), key=lambda p: p.stem):
            if start_after and path.stem <= start_after:
                continue
            try:
                yield json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("Skipping corrupt document", extra={"path": str(path)})

    def update_documents(self, updates: list[tuple[str, dict]]) -> None:
        for doc_id, fields in updates:
            path = self.docs_dir / f"{doc_id}.json"
            if not path.exists():
                continue
            record = json.loads(path.read_text(encoding="utf-8"))
            record.update(fields)
            path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    def replace_documents(self, updates: list[tuple[str, dict]]) -> None:
        # Locally, dict.update already sets each top-level field to exactly the
        # given value (a nested map replaces the old one), which is the wholesale
        # semantics we want. Firestore is where this must differ from a merge.
        self.update_documents(updates)

    def _downloaded(self, site: str) -> list[dict]:
        records = []
        for path in self.docs_dir.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if record.get("site") == site and record.get("state") == "DOWNLOADED":
                records.append(record)
        return records

    def latest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        records = self._downloaded(site)
        records.sort(key=lambda r: r.get("downloaded_at") or "", reverse=True)
        return records[:limit]

    def highest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        records = self._downloaded(site)
        records.sort(key=lambda r: r.get("sort_key") or "", reverse=True)
        return records[:limit]


class FirestoreMetadataRepository(MetadataRepository):
    def __init__(self, cfg: MetadataConfig):
        from google.cloud import firestore

        self._client = firestore.Client(project=cfg.project, database=cfg.database)
        self._docs = self._client.collection(cfg.documents_collection)
        self._jobs = self._client.collection(cfg.jobs_collection)
        # One document per site, keyed by site name: resume is a single point
        # read, with no composite index or ordered query required.
        self._checkpoints = self._client.collection("checkpoints")
        self._batch_size = min(cfg.batch_size, 500)
        self._pending: dict[str, DocumentRecord] = {}
        self._lock = threading.Lock()
        log.info(
            "Using Firestore metadata",
            extra={"project": cfg.project, "database": cfg.database},
        )

    def get_document(self, doc_id: str) -> dict | None:
        with self._lock:
            if doc_id in self._pending:
                return self._pending[doc_id].to_dict()
        snapshot = self._docs.document(doc_id).get()
        return snapshot.to_dict() if snapshot.exists else None

    def stage_document(self, record: DocumentRecord) -> None:
        with self._lock:
            self._pending[record.doc_id] = record
            ready = len(self._pending) >= self._batch_size
        if ready:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            pending, self._pending = self._pending, {}
        if not pending:
            return
        items = list(pending.items())
        for start in range(0, len(items), 500):
            batch = self._client.batch()
            for doc_id, record in items[start : start + 500]:
                # merge=True so a re-crawl updates state without clobbering
                # attributes an earlier pass recorded.
                batch.set(self._docs.document(doc_id), record.to_dict(), merge=True)
            batch.commit()
        log.info("Flushed metadata to Firestore", extra={"count": len(items)})

    def save_job(self, job: JobRecord) -> None:
        self._jobs.document(job.job_id).set(job.to_dict(), merge=True)

    def find_running_jobs(self, limit: int = 200) -> list[dict]:
        from google.cloud import firestore

        try:
            query = self._jobs.where(
                filter=firestore.FieldFilter("status", "==", "RUNNING")
            ).limit(limit)
            return [doc.to_dict() for doc in query.stream()]
        except Exception as exc:
            log.warning("Could not query running jobs", extra={"error": str(exc)})
            return []

    def update_job(self, job_id: str, fields: dict) -> None:
        self._jobs.document(job_id).set(fields, merge=True)

    def get_checkpoint(self, checkpoint_id: str) -> dict | None:
        snapshot = self._checkpoints.document(checkpoint_id).get()
        return snapshot.to_dict() if snapshot.exists else None

    def save_checkpoint(self, checkpoint_id: str, checkpoint: CheckpointRecord) -> None:
        self._checkpoints.document(checkpoint_id).set(checkpoint.to_dict(), merge=True)

    def iter_documents(self, start_after: str | None = None, page_size: int = 500):
        from google.cloud import firestore

        cursor = start_after
        while True:
            query = self._docs.order_by("__name__").limit(page_size)
            if cursor:
                query = query.start_after({"__name__": self._docs.document(cursor)})
            page = list(query.stream())
            if not page:
                return
            for snapshot in page:
                record = snapshot.to_dict() or {}
                record.setdefault("doc_id", snapshot.id)
                yield record
            cursor = page[-1].id
            if len(page) < page_size:
                return

    def update_documents(self, updates: list[tuple[str, dict]]) -> None:
        for start in range(0, len(updates), 500):
            batch = self._client.batch()
            for doc_id, fields in updates[start : start + 500]:
                batch.set(self._docs.document(doc_id), fields, merge=True)
            batch.commit()

    def replace_documents(self, updates: list[tuple[str, dict]]) -> None:
        # batch.update (as opposed to set(merge=True)) assigns each field
        # wholesale: a map-valued field is replaced entirely, so keys omitted
        # from the new map are deleted. This is how a legacy nested key like
        # attributes.language is removed rather than left behind.
        for start in range(0, len(updates), 500):
            batch = self._client.batch()
            for doc_id, fields in updates[start : start + 500]:
                batch.update(self._docs.document(doc_id), fields)
            batch.commit()

    def _query_downloaded(self, site: str, order_field: str, limit: int) -> list[dict]:
        """One ordered, indexed query — no collection scan, no bucket listing."""
        from google.api_core import exceptions as gexc
        from google.cloud import firestore

        try:
            query = (
                self._docs.where(filter=firestore.FieldFilter("site", "==", site))
                .where(filter=firestore.FieldFilter("state", "==", "DOWNLOADED"))
                .order_by(order_field, direction=firestore.Query.DESCENDING)
                .limit(limit)
            )
            return [doc.to_dict() for doc in query.stream()]
        except gexc.FailedPrecondition as exc:
            # Almost always a missing composite index. Say so explicitly —
            # the caller falls back to a slower path, which is easy to mistake
            # for the fast one working badly.
            log.error(
                "Firestore rejected the resume query; deploy the composite "
                "indexes (firestore.indexes.json) to enable fast resume",
                extra={"orderField": order_field, "error": str(exc)},
            )
            return []
        except Exception as exc:
            log.warning(
                "Resume query failed", extra={"orderField": order_field, "error": str(exc)}
            )
            return []

    def latest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        return self._query_downloaded(site, "downloaded_at", limit)

    def highest_downloaded(self, site: str, limit: int = 5) -> list[dict]:
        return self._query_downloaded(site, "sort_key", limit)


def build_repository(cfg: MetadataConfig) -> MetadataRepository:
    if cfg.backend == "firestore":
        return FirestoreMetadataRepository(cfg)
    log.info("Using local metadata store", extra={"root": cfg.local_root})
    return LocalMetadataRepository(cfg.local_root, cfg.documents_collection, cfg.jobs_collection)
