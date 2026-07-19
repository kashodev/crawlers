"""Migration runner.

Applies ordered migrations — Firestore indexes, data backfills — exactly once,
recording each in a ledger. Runs before the crawler jobs are updated, as its
own Cloud Run Job, so it uses the deployment's service account rather than
whoever happens to be running deploy.sh.

    python -m migrations.runner --status
    python -m migrations.runner --apply
    python -m migrations.runner --apply --dry-run

Idempotency has three layers, because any one of them can be defeated:

  1. The ledger records what already ran, keyed by migration id.
  2. A transactional claim stops two concurrent runners applying the same
     migration (Firestore only; local runs are single-process).
  3. Migrations are written to be re-runnable on their own — index creation
     tolerates ALREADY_EXISTS, the backfill skips documents that already have
     the field. The ledger is then an optimisation and an audit trail rather
     than the only thing standing between you and a double-apply.

A migration whose source changes after being applied is reported as DRIFTED
rather than silently skipped or blindly re-run.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import logging
import os
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from common.env import load_env

load_env()

from common import config as config_module  # noqa: E402
from common.metadata import MetadataRepository, build_repository  # noqa: E402
from utils.logging_config import configure_logging  # noqa: E402

log = logging.getLogger("migrations")

MIGRATIONS_DIR = Path(__file__).resolve().parent
ROOT = MIGRATIONS_DIR.parent
LEDGER_COLLECTION = "_migrations"

# A claim older than this is treated as abandoned (the runner was killed
# mid-migration), so a later run may take it over.
STALE_CLAIM_AFTER = timedelta(minutes=30)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MigrationContext:
    """Everything a migration is allowed to touch."""

    project: str | None
    database: str
    repo: MetadataRepository
    firestore_enabled: bool
    dry_run: bool
    index_timeout_seconds: int
    log: logging.Logger
    # Persist partial progress so a long migration resumes rather than restarts.
    save_progress: Callable[[dict[str, Any]], None] = lambda _: None
    progress: dict[str, Any] = field(default_factory=dict)


@dataclass
class Migration:
    id: str
    description: str
    kind: str  # "declarative" | "data"
    checksum: str
    apply: Callable[[MigrationContext], dict[str, Any] | None]
    path: Path


def discover() -> list[Migration]:
    """Load NNNN_*.py migration modules in numeric order.

    Loaded by path because the filenames start with digits and so are not
    importable as module names.
    """
    migrations: list[Migration] = []
    for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9][0-9]_*.py")):
        spec = importlib.util.spec_from_file_location(f"migration_{path.stem}", path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load migration {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr in ("ID", "DESCRIPTION", "apply"):
            if not hasattr(module, attr):
                raise AttributeError(f"{path.name} must define {attr}")

        migrations.append(
            Migration(
                id=module.ID,
                description=module.DESCRIPTION,
                kind=getattr(module, "KIND", "data"),
                # Checksum the source, so an edit after apply is detectable.
                checksum=hashlib.sha256(path.read_bytes()).hexdigest()[:16],
                apply=module.apply,
                path=path,
            )
        )
    return migrations


class Ledger:
    """Records which migrations have run. Firestore when configured, else files."""

    def __init__(self, repo: MetadataRepository, project: str | None, database: str):
        self._client = None
        self._local_dir: Path | None = None

        if project:
            from google.cloud import firestore

            self._client = firestore.Client(project=project, database=database)
            self._collection = self._client.collection(LEDGER_COLLECTION)
        else:
            root = Path(getattr(repo, "docs_dir", ROOT / "data/_metadata/documents")).parent
            self._local_dir = root / LEDGER_COLLECTION
            self._local_dir.mkdir(parents=True, exist_ok=True)

    def get(self, migration_id: str) -> dict | None:
        if self._client is not None:
            snapshot = self._collection.document(migration_id).get()
            return snapshot.to_dict() if snapshot.exists else None
        path = self._local_dir / f"{migration_id}.json"  # type: ignore[operator]
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def _write(self, migration_id: str, record: dict) -> None:
        if self._client is not None:
            self._collection.document(migration_id).set(record, merge=True)
        else:
            path = self._local_dir / f"{migration_id}.json"  # type: ignore[operator]
            path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    def claim(self, migration: Migration) -> tuple[bool, str]:
        """Try to take ownership of a migration.

        Returns (claimed, reason). On Firestore this is a transaction, so two
        runners racing on the same deploy cannot both win.
        """
        if self._client is None:
            record = self.get(migration.id)
            allowed, reason = _claim_decision(record, migration)
            if allowed:
                self._write(migration.id, _running_record(record, migration))
            return allowed, reason

        from google.cloud import firestore

        doc_ref = self._collection.document(migration.id)
        decision: dict[str, Any] = {}

        @firestore.transactional
        def attempt(transaction):
            snapshot = doc_ref.get(transaction=transaction)
            record = snapshot.to_dict() if snapshot.exists else None
            allowed, reason = _claim_decision(record, migration)
            decision["allowed"], decision["reason"] = allowed, reason
            if allowed:
                transaction.set(doc_ref, _running_record(record, migration), merge=True)

        attempt(self._client.transaction())
        return decision.get("allowed", False), decision.get("reason", "unknown")

    def complete(self, migration: Migration, result: dict | None) -> None:
        self._write(
            migration.id,
            {
                "status": "COMPLETED",
                "finished_at": _now(),
                "result": result or {},
                "error": None,
            },
        )

    def fail(self, migration: Migration, error: str, progress: dict | None = None) -> None:
        record = {"status": "FAILED", "finished_at": _now(), "error": error}
        if progress:
            record["progress"] = progress
        self._write(migration.id, record)

    def save_progress(self, migration_id: str, progress: dict) -> None:
        self._write(migration_id, {"progress": progress, "updated_at": _now()})


def _claim_decision(record: dict | None, migration: Migration) -> tuple[bool, str]:
    """Whether `migration` may run, given its ledger record."""
    if record is None:
        return True, "not yet applied"

    status = record.get("status")
    if status == "COMPLETED":
        if record.get("checksum") != migration.checksum:
            # Changed after being applied. Re-running could be destructive and
            # skipping hides a real divergence, so surface it and do neither.
            return False, "DRIFTED: source changed since it was applied"
        return False, "already applied"

    if status == "RUNNING":
        updated = record.get("updated_at") or record.get("started_at")
        try:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(updated)
        except (TypeError, ValueError):
            age = STALE_CLAIM_AFTER
        if age < STALE_CLAIM_AFTER:
            return False, "already running elsewhere"
        return True, "taking over an abandoned run"

    return True, f"retrying after {status or 'unknown'}"


def _running_record(previous: dict | None, migration: Migration) -> dict:
    return {
        "id": migration.id,
        "description": migration.description,
        "kind": migration.kind,
        "checksum": migration.checksum,
        "status": "RUNNING",
        "started_at": _now(),
        "updated_at": _now(),
        "attempts": int((previous or {}).get("attempts") or 0) + 1,
        "applied_by": f"{socket.gethostname()}:{os.getpid()}",
        "progress": (previous or {}).get("progress") or {},
    }


def build_context(args) -> tuple[MigrationContext, Ledger]:
    # Any site config supplies the shared Firestore/collection settings; the
    # migrations operate across sites, not on one crawl.
    cfg = config_module.load(str(ROOT / "toronto_bylaws" / "config.yaml"))
    repo = build_repository(cfg.metadata)
    project = cfg.metadata.project if cfg.metadata.backend == "firestore" else None

    context = MigrationContext(
        project=project,
        database=cfg.metadata.database,
        repo=repo,
        firestore_enabled=bool(project),
        dry_run=args.dry_run,
        index_timeout_seconds=args.index_timeout,
        log=log,
    )
    return context, Ledger(repo, project, cfg.metadata.database)


def run(args) -> int:
    migrations = discover()
    context, ledger = build_context(args)

    log.info(
        "Migration runner starting",
        extra={
            "migrations": len(migrations),
            "backend": "firestore" if context.firestore_enabled else "local",
            "project": context.project,
            "dryRun": args.dry_run,
        },
    )

    failures = 0
    drifted = 0

    for migration in migrations:
        record = ledger.get(migration.id) or {}

        if args.status:
            print(
                f"{migration.id}  {record.get('status', 'PENDING'):<10} "
                f"{migration.kind:<12} {migration.description}"
            )
            continue

        if args.dry_run:
            # A dry run must leave no trace: claiming or completing here would
            # mark the migration applied and cause the real run to skip it.
            allowed, reason = _claim_decision(record or None, migration)
            if not allowed:
                log.info("Would skip migration", extra={"id": migration.id, "reason": reason})
                continue
            log.info("[dry-run] applying migration", extra={"id": migration.id})
            context.progress = record.get("progress") or {}
            context.save_progress = lambda _p: None
            try:
                result = migration.apply(context)
                log.info(
                    "[dry-run] migration would apply",
                    extra={"id": migration.id, "result": result or {}},
                )
            except Exception:
                failures += 1
                log.exception("[dry-run] migration failed", extra={"id": migration.id})
                break
            continue

        claimed, reason = ledger.claim(migration)
        if not claimed:
            level = log.warning if reason.startswith("DRIFTED") else log.info
            level("Skipping migration", extra={"id": migration.id, "reason": reason})
            if reason.startswith("DRIFTED"):
                drifted += 1
            continue

        log.info(
            "Applying migration",
            extra={"id": migration.id, "kind": migration.kind, "reason": reason},
        )
        context.progress = record.get("progress") or {}
        context.save_progress = lambda p, _id=migration.id: ledger.save_progress(_id, p)

        try:
            result = migration.apply(context)
            ledger.complete(migration, result)
            log.info("Migration applied", extra={"id": migration.id, "result": result or {}})
        except Exception as exc:
            failures += 1
            ledger.fail(migration, f"{type(exc).__name__}: {exc}", context.progress)
            log.exception("Migration failed", extra={"id": migration.id})
            # Later migrations may depend on this one; stop rather than compound it.
            break

    if drifted:
        log.warning(
            "Some migrations drifted from their applied version; "
            "review them and either revert the edit or add a new migration",
            extra={"drifted": drifted},
        )
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply Firestore migrations.")
    parser.add_argument("--apply", action="store_true", help="apply pending migrations")
    parser.add_argument("--status", action="store_true", help="list migrations and their state")
    parser.add_argument(
        "--dry-run", action="store_true", help="report what would change without writing"
    )
    parser.add_argument(
        "--index-timeout",
        type=int,
        default=int(os.environ.get("MIGRATION_INDEX_TIMEOUT", "600")),
        help="seconds to wait for index builds before warning (default 600)",
    )
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    if not (args.apply or args.status):
        parser.error("one of --apply or --status is required")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
