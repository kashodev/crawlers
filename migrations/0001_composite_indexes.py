"""Create the composite indexes the resume queries depend on.

Reads firestore.indexes.json — the same file the Firebase CLI would consume —
and reconciles it against the live indexes using the Firestore Admin API, so
the container needs no gcloud binary.

Idempotent on its own: existing indexes are matched on their field definitions
and left alone, and a create that races another runner is tolerated.

Waiting for READY is deliberately soft. Index builds can take from minutes to
over an hour on a populated collection, and the crawler already degrades to a
slower resume path while they build. Blocking a deploy on that trades a real
cost for a theoretical one, so a timeout here warns rather than fails.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

ID = "0001_composite_indexes"
DESCRIPTION = "Create composite indexes for resume queries"
KIND = "declarative"

INDEX_FILE = Path(__file__).resolve().parent.parent / "firestore.indexes.json"


def _signature(collection: str, fields) -> tuple:
    """Comparable identity for an index: collection + ordered field list.

    Firestore appends an implicit __name__ field to every composite index, so
    it is excluded — otherwise a declared index never matches its live twin.
    """
    normalized = tuple(
        (f["fieldPath"], f.get("order", "ASCENDING"))
        for f in fields
        if f["fieldPath"] != "__name__"
    )
    return (collection, normalized)


def _declared() -> list[dict]:
    if not INDEX_FILE.exists():
        raise FileNotFoundError(f"missing {INDEX_FILE}")
    spec = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    # "//" keys in the file are comments and are simply ignored here.
    return list(spec.get("indexes", []))


def apply(ctx) -> dict:
    if not ctx.firestore_enabled:
        ctx.log.info("Local backend: no Firestore indexes to create; skipping")
        return {"skipped": "local backend"}

    from google.api_core import exceptions as gexc
    from google.cloud import firestore_admin_v1 as admin

    client = admin.FirestoreAdminClient()
    declared = _declared()

    created, existing = [], []

    for index_spec in declared:
        collection = index_spec["collectionGroup"]
        parent = (
            f"projects/{ctx.project}/databases/{ctx.database}"
            f"/collectionGroups/{collection}"
        )
        wanted = _signature(collection, index_spec["fields"])

        live = {
            _signature(collection, [
                {"fieldPath": f.field_path, "order": admin.Index.IndexField.Order(f.order).name}
                for f in idx.fields
                if f.field_path != "__name__"
            ])
            for idx in client.list_indexes(parent=parent)
        }

        if wanted in live:
            existing.append(wanted[1])
            ctx.log.info("Index already present", extra={"collection": collection})
            continue

        if ctx.dry_run:
            ctx.log.info("[dry-run] would create index", extra={"index": str(wanted)})
            created.append(wanted[1])
            continue

        index = admin.Index(
            query_scope=admin.Index.QueryScope.COLLECTION,
            fields=[
                admin.Index.IndexField(
                    field_path=f["fieldPath"],
                    order=admin.Index.IndexField.Order[f.get("order", "ASCENDING")],
                )
                for f in index_spec["fields"]
                if f["fieldPath"] != "__name__"
            ],
        )
        try:
            client.create_index(parent=parent, index=index)
            created.append(wanted[1])
            ctx.log.info("Index creation started", extra={"collection": collection})
        except gexc.AlreadyExists:
            # Another runner won the race; that is a success for our purposes.
            existing.append(wanted[1])
            ctx.log.info("Index already exists", extra={"collection": collection})

    if ctx.dry_run:
        return {"created": len(created), "existing": len(existing), "dryRun": True}

    ready = _wait_for_ready(ctx, client, {i["collectionGroup"] for i in declared})
    return {"created": len(created), "existing": len(existing), "allReady": ready}


def _wait_for_ready(ctx, client, collections: set[str]) -> bool:
    """Poll until no index is still building, or the timeout elapses.

    Returns True if everything reached READY. A False return is not an error:
    the caller keeps going and the crawler degrades until the build finishes.
    """
    from google.cloud import firestore_admin_v1 as admin

    deadline = time.monotonic() + ctx.index_timeout_seconds
    interval = 10

    while True:
        building = []
        for collection in collections:
            parent = (
                f"projects/{ctx.project}/databases/{ctx.database}"
                f"/collectionGroups/{collection}"
            )
            for idx in client.list_indexes(parent=parent):
                if idx.state != admin.Index.State.READY:
                    building.append((collection, admin.Index.State(idx.state).name))

        if not building:
            ctx.log.info("All indexes READY")
            return True

        if time.monotonic() >= deadline:
            ctx.log.warning(
                "Indexes still building at timeout; continuing anyway. Resume "
                "queries fall back to a slower path until they are READY. "
                "Check: gcloud firestore indexes composite list",
                extra={"stillBuilding": building, "timeoutSeconds": ctx.index_timeout_seconds},
            )
            return False

        ctx.log.info("Waiting for indexes", extra={"building": building})
        time.sleep(interval)
