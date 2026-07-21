"""Backfill a `locale` language tag onto every document's attributes.

Documents can be multilingual — some source PDFs lay English and French out in
two document-wide columns — so language is modelled as a *list* of BCP 47
language tags (e.g. `["en-CA"]`, or `["en-CA", "fr-CA"]` for a bilingual doc),
which is the standard, query-friendly representation. A scalar would have to
become a list the first time a second language appeared; a list never has to
change shape.

Every document crawled so far is English-only Canadian content, so this sets
`attributes.locale = ["en-CA"]` across the board. It always overwrites, so a
stale value is replaced rather than skipped, and it drops the legacy
`attributes.language` scalar (an "e"/"f" flag set by the bills crawler) that
`locale` supersedes — using replace_documents so the removal propagates on
Firestore as well as locally.

Idempotent: re-running produces the identical `["en-CA"]` and no `language`
key. Progress is checkpointed by document id so a large collection resumes
rather than restarts.
"""

from __future__ import annotations

ID = "0003_backfill_locale"
DESCRIPTION = "Backfill attributes.locale=['en-CA'] and drop the legacy language flag"
KIND = "data"

BATCH_SIZE = 400
PROGRESS_EVERY = 2000

# All content to date is English-only Canadian. When a genuinely bilingual
# source is added, its crawler should set `locale` directly; this backfill only
# establishes the baseline for the existing English corpus.
DEFAULT_LOCALE = ["en-CA"]

# Legacy per-document language flags that `locale` replaces. Removed when found.
LEGACY_KEYS = ("language", "lang")


def _new_attributes(attributes: dict) -> dict:
    """Return the attributes map with locale set and legacy language keys gone."""
    updated = {k: v for k, v in attributes.items() if k not in LEGACY_KEYS}
    updated["locale"] = list(DEFAULT_LOCALE)
    return updated


def _needs_update(attributes: dict) -> bool:
    """Whether this document already matches the target shape (for reporting)."""
    if attributes.get("locale") != DEFAULT_LOCALE:
        return True
    return any(k in attributes for k in LEGACY_KEYS)


def apply(ctx) -> dict:
    start_after = (ctx.progress or {}).get("last_doc_id")
    if start_after:
        ctx.log.info("Resuming locale backfill", extra={"afterDocId": start_after})

    scanned = updated = already = 0
    pending: list[tuple[str, dict]] = []
    last_doc_id = start_after

    def flush():
        nonlocal pending
        if pending and not ctx.dry_run:
            # Wholesale replace of the attributes map so the legacy language key
            # is actually removed, not merged around.
            ctx.repo.replace_documents(pending)
        pending = []

    for record in ctx.repo.iter_documents(start_after=start_after):
        scanned += 1
        doc_id = record.get("doc_id")
        last_doc_id = doc_id
        attributes = record.get("attributes") or {}

        if not _needs_update(attributes):
            already += 1
        else:
            pending.append((doc_id, {"attributes": _new_attributes(attributes)}))
            updated += 1

        if len(pending) >= BATCH_SIZE:
            flush()

        if scanned % PROGRESS_EVERY == 0:
            flush()
            ctx.progress = {"last_doc_id": last_doc_id, "updated": updated}
            ctx.save_progress(ctx.progress)
            ctx.log.info(
                "Locale backfill progress",
                extra={"scanned": scanned, "updated": updated, "already": already},
            )

    flush()

    result = {
        "scanned": scanned,
        "updated": updated,
        "alreadyCorrect": already,
        "locale": DEFAULT_LOCALE,
        "dryRun": ctx.dry_run,
    }
    ctx.log.info("Locale backfill complete", extra=result)
    return result
