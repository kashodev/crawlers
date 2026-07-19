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
    def get_checkpoint(self, site: str) -> dict | None: ...

    @abstractmethod
    def save_checkpoint(self, checkpoint: CheckpointRecord) -> None: ...

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

    def get_checkpoint(self, site: str) -> dict | None:
        path = self.checkpoints_dir / f"{site}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("Corrupt checkpoint; ignoring", extra={"site": site})
            return None

    def save_checkpoint(self, checkpoint: CheckpointRecord) -> None:
        (self.checkpoints_dir / f"{checkpoint.site}.json").write_text(
            json.dumps(checkpoint.to_dict(), indent=2), encoding="utf-8"
        )


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

    def get_checkpoint(self, site: str) -> dict | None:
        snapshot = self._checkpoints.document(site).get()
        return snapshot.to_dict() if snapshot.exists else None

    def save_checkpoint(self, checkpoint: CheckpointRecord) -> None:
        self._checkpoints.document(checkpoint.site).set(checkpoint.to_dict(), merge=True)


def build_repository(cfg: MetadataConfig) -> MetadataRepository:
    if cfg.backend == "firestore":
        return FirestoreMetadataRepository(cfg)
    log.info("Using local metadata store", extra={"root": cfg.local_root})
    return LocalMetadataRepository(cfg.local_root, cfg.documents_collection, cfg.jobs_collection)
