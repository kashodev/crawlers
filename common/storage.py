"""PDF storage backends: local disk for development, GCS for Cloud Run.

Both implement the same interface, so a crawler never knows where its bytes
land. `exists()` is the first line of idempotency — a re-run skips objects that
are already stored before it spends a request on the origin.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from common.config import StorageConfig

log = logging.getLogger(__name__)


class Storage(ABC):
    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def write(self, key: str, data: bytes, content_type: str = "application/pdf") -> str:
        """Store bytes and return a URI identifying the stored object."""

    @abstractmethod
    def uri(self, key: str) -> str: ...

    @abstractmethod
    def list_keys(self, sub_prefix: str = "") -> Iterator[str]:
        """Yield stored keys under `sub_prefix`, relative to the configured prefix.

        Always pass a sub_prefix when one is known (a year, a session): an
        unscoped listing walks every object in the bucket, which is tens of
        thousands of round trips once a backfill has run.
        """


class LocalStorage(Storage):
    def __init__(self, root: str, prefix: str = ""):
        self.root = Path(root)
        self.prefix = prefix.strip("/")
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        rel = f"{self.prefix}/{key}" if self.prefix else key
        # Guard against a crafted key escaping the data root via '..'.
        resolved = (self.root / rel).resolve()
        if not str(resolved).startswith(str(self.root.resolve())):
            raise ValueError(f"storage key escapes root: {key}")
        return resolved

    def exists(self, key: str) -> bool:
        path = self._path(key)
        return path.exists() and path.stat().st_size > 0

    def write(self, key: str, data: bytes, content_type: str = "application/pdf") -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename: an interrupted run never leaves a truncated PDF
        # that a later run would mistake for a complete download.
        tmp = path.with_suffix(path.suffix + ".part")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        log.debug("Wrote local file", extra={"path": str(path), "bytes": len(data)})
        return str(path)

    def uri(self, key: str) -> str:
        return str(self._path(key))

    def list_keys(self, sub_prefix: str = "") -> Iterator[str]:
        root = self._path("")
        if not root.exists():
            return
        base = (root / sub_prefix) if sub_prefix else root
        if not base.exists():
            return
        for path in base.rglob("*"):
            if path.is_file() and not path.name.endswith(".part"):
                yield str(path.relative_to(root))


class GCSStorage(Storage):
    def __init__(self, bucket: str, prefix: str = ""):
        from google.cloud import storage as gcs

        self.bucket_name = bucket
        self.prefix = prefix.strip("/")
        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket)

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        return self._bucket.blob(self._key(key)).exists()

    def write(self, key: str, data: bytes, content_type: str = "application/pdf") -> str:
        blob = self._bucket.blob(self._key(key))
        blob.upload_from_string(data, content_type=content_type)
        log.debug("Uploaded to GCS", extra={"uri": self.uri(key), "bytes": len(data)})
        return self.uri(key)

    def uri(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self._key(key)}"

    def list_keys(self, sub_prefix: str = "") -> Iterator[str]:
        offset = len(self.prefix) + 1 if self.prefix else 0
        full_prefix = self._key(sub_prefix) if sub_prefix else (self.prefix or None)
        for blob in self._client.list_blobs(self._bucket, prefix=full_prefix):
            yield blob.name[offset:]


def build_storage(cfg: StorageConfig) -> Storage:
    if cfg.backend == "gcs":
        if not cfg.bucket:
            raise ValueError("storage.backend='gcs' requires a bucket (set PDF_BUCKET)")
        log.info("Using GCS storage", extra={"bucket": cfg.bucket, "prefix": cfg.prefix})
        return GCSStorage(cfg.bucket, cfg.prefix)
    log.info("Using local storage", extra={"root": cfg.local_root})
    return LocalStorage(cfg.local_root, cfg.prefix)
