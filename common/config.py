"""Crawl configuration.

One YAML file per site, loaded from disk locally or from GCS in Cloud Run
(CONFIG_BUCKET). Config drives seed URLs and every politeness/limit knob, so a
behaviour change is a config edit and redeploy rather than a code change.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)


@dataclass
class PolitenessConfig:
    user_agent: str = (
        "pdf-research-crawler/1.0 (+RAG test-data collection; contact: kasho.dev@gmail.com)"
    )
    # Floor between requests to one host. robots.txt Crawl-delay wins if larger.
    min_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    respect_robots: bool = True
    # Concurrency is per-host and deliberately low: these are public-sector
    # servers and the crawl is not latency-sensitive.
    max_concurrent_requests: int = 2
    jitter_seconds: float = 0.3


@dataclass
class RetryConfig:
    max_attempts: int = 4
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0
    # 429/503 mean "you are going too fast" -> always worth retrying with backoff.
    retry_status_codes: tuple[int, ...] = (408, 429, 500, 502, 503, 504)
    timeout_seconds: float = 60.0
    # PDFs are large; give the body its own, longer budget.
    download_timeout_seconds: float = 300.0


@dataclass
class LimitsConfig:
    # Hard stop after this many consecutive probes that yield no PDF.
    consecutive_miss_limit: int = 1000
    # Per-listing-unit tolerance (e.g. gaps within one Toronto bylaw year).
    per_unit_miss_limit: int = 50
    max_documents: int | None = None
    max_pages: int | None = None
    max_pdf_bytes: int = 512 * 1024 * 1024


@dataclass
class StorageConfig:
    # "local" writes into data/; "gcs" writes to bucket. Set by env in Cloud Run.
    backend: str = "local"
    local_root: str = "data"
    bucket: str | None = None
    prefix: str = ""


@dataclass
class MetadataConfig:
    backend: str = "local"  # "local" (JSON files) or "firestore"
    project: str | None = None
    database: str = "(default)"
    documents_collection: str = "documents"
    jobs_collection: str = "jobs"
    local_root: str = "data/_metadata"
    batch_size: int = 400  # Firestore hard-caps a batch at 500 writes.


@dataclass
class SiteConfig:
    name: str
    module: str
    seeds: list[str] = field(default_factory=list)
    enabled: bool = True
    politeness: PolitenessConfig = field(default_factory=PolitenessConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    # Crawler-specific knobs (year ranges, URL templates, regexes).
    options: dict[str, Any] = field(default_factory=dict)


def _build(cls, data: dict[str, Any] | None):
    """Instantiate a config dataclass, ignoring unknown keys with a warning."""
    if not data:
        return cls()
    known = {f.name for f in fields(cls)}
    unknown = set(data) - known
    if unknown:
        log.warning("Ignoring unknown config keys for %s: %s", cls.__name__, sorted(unknown))
    return cls(**{k: v for k, v in data.items() if k in known})


def from_dict(data: dict[str, Any]) -> SiteConfig:
    if "name" not in data:
        raise ValueError("site config requires 'name'")
    if "module" not in data:
        raise ValueError(f"site config '{data['name']}' requires 'module'")

    cfg = SiteConfig(
        name=data["name"],
        module=data["module"],
        seeds=list(data.get("seeds") or []),
        enabled=bool(data.get("enabled", True)),
        politeness=_build(PolitenessConfig, data.get("politeness")),
        retry=_build(RetryConfig, data.get("retry")),
        limits=_build(LimitsConfig, data.get("limits")),
        storage=_build(StorageConfig, data.get("storage")),
        metadata=_build(MetadataConfig, data.get("metadata")),
        options=dict(data.get("options") or {}),
    )
    return _apply_env_overrides(cfg)


def _apply_env_overrides(cfg: SiteConfig) -> SiteConfig:
    """Environment wins over file config.

    This is what lets one image run unchanged locally and on Cloud Run: the job
    spec sets PDF_BUCKET/FIRESTORE_PROJECT and the same YAML switches backends.
    """
    if bucket := os.environ.get("PDF_BUCKET"):
        cfg.storage.backend = "gcs"
        cfg.storage.bucket = bucket
    if prefix := os.environ.get("PDF_PREFIX"):
        cfg.storage.prefix = prefix
    if project := os.environ.get("FIRESTORE_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT"):
        cfg.metadata.backend = "firestore"
        cfg.metadata.project = project
    if db := os.environ.get("FIRESTORE_DATABASE"):
        cfg.metadata.database = db
    if ua := os.environ.get("CRAWLER_USER_AGENT"):
        cfg.politeness.user_agent = ua
    if delay := os.environ.get("CRAWLER_MIN_DELAY"):
        cfg.politeness.min_delay_seconds = float(delay)
    if limit := os.environ.get("CRAWLER_MAX_DOCUMENTS"):
        cfg.limits.max_documents = int(limit)
    return cfg


def load_file(path: str | Path) -> SiteConfig:
    with open(path, "r", encoding="utf-8") as fh:
        return from_dict(yaml.safe_load(fh) or {})


def load(path_or_uri: str) -> SiteConfig:
    """Load a site config from a local path or a gs:// URI."""
    if path_or_uri.startswith("gs://"):
        from google.cloud import storage as gcs  # imported lazily; local runs skip it

        bucket_name, _, blob_name = path_or_uri[5:].partition("/")
        client = gcs.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        log.info("Loading config from GCS", extra={"uri": path_or_uri})
        return from_dict(yaml.safe_load(blob.download_as_text()) or {})
    return load_file(path_or_uri)
