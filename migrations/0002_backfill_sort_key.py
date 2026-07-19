"""Backfill `sort_key` on documents written before the field existed.

The frontier resume query orders on `sort_key`, and Firestore does not index a
field a document lacks — so without this, every pre-existing document is
invisible to that query and resume falls back to the cursor alone.

The value is derived from each document's `storage_key` using the same
position helpers the crawlers use, so backfilled rows sort identically to newly
written ones.

Idempotent on its own: documents that already have a `sort_key` are skipped, so
re-running is a no-op. Progress is checkpointed by document id, so a run killed
partway resumes rather than starting over — the collection can hold 100k+
documents.
"""

from __future__ import annotations

import importlib

ID = "0002_backfill_sort_key"
DESCRIPTION = "Backfill sort_key on documents created before the field existed"
KIND = "data"

BATCH_SIZE = 400
# How often to persist the resume cursor, in documents scanned.
PROGRESS_EVERY = 2000

# Site name (as written on documents) -> module holding its position helpers.
SITE_MODULES = {
    "toronto-bylaws": "toronto_bylaws.crawler",
    "ontario-legislative-bills": "ontario_legislative_bills.crawler",
}


def _helpers():
    helpers = {}
    for site, module_name in SITE_MODULES.items():
        module = importlib.import_module(module_name)
        helpers[site] = (module.position_from_key, module.position_sort_key)
    return helpers


def apply(ctx) -> dict:
    helpers = _helpers()
    start_after = (ctx.progress or {}).get("last_doc_id")
    if start_after:
        ctx.log.info("Resuming backfill", extra={"afterDocId": start_after})

    scanned = updated = skipped = unmapped = 0
    pending: list[tuple[str, dict]] = []
    last_doc_id = start_after

    def flush():
        nonlocal pending
        if not pending:
            return
        if not ctx.dry_run:
            ctx.repo.update_documents(pending)
        pending = []

    for record in ctx.repo.iter_documents(start_after=start_after):
        scanned += 1
        doc_id = record.get("doc_id")
        last_doc_id = doc_id

        if record.get("sort_key"):
            skipped += 1
        else:
            site = record.get("site")
            storage_key = record.get("storage_key")
            pair = helpers.get(site)

            if pair and storage_key:
                position_from_key, position_sort_key = pair
                position = position_from_key(storage_key)
                if position:
                    pending.append((doc_id, {"sort_key": position_sort_key(position)}))
                    updated += 1
                else:
                    unmapped += 1
            else:
                # No storage_key (never downloaded) or an unknown site: nothing
                # to derive a position from, so leave it alone.
                unmapped += 1

        if len(pending) >= BATCH_SIZE:
            flush()

        if scanned % PROGRESS_EVERY == 0:
            flush()
            ctx.progress = {"last_doc_id": last_doc_id, "updated": updated}
            ctx.save_progress(ctx.progress)
            ctx.log.info(
                "Backfill progress",
                extra={"scanned": scanned, "updated": updated, "skipped": skipped},
            )

    flush()

    result = {
        "scanned": scanned,
        "updated": updated,
        "alreadyHadKey": skipped,
        "noPosition": unmapped,
        "dryRun": ctx.dry_run,
    }
    ctx.log.info("Backfill complete", extra=result)
    return result
