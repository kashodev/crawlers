"""Language tagging for crawled documents.

Documents record language as a list of BCP 47 tags in `attributes.locale`
(e.g. ["en-CA"], or ["en-CA", "fr-CA"] for a bilingual document). This is the
same shape migration 0003 backfilled onto older documents, so freshly crawled
documents and backfilled ones stay consistent. Keep new crawlers writing
`locale` (never the legacy scalar `language`) so the two never diverge again.
"""

from __future__ import annotations

# Canadian English is the baseline: every source crawled so far is en-CA.
EN_CA = "en-CA"
FR_CA = "fr-CA"

# Source-specific language codes (e.g. the bills site's "e"/"f") -> BCP 47 tag.
_SOURCE_TAG = {
    "e": EN_CA,
    "en": EN_CA,
    "f": FR_CA,
    "fr": FR_CA,
}


def to_locale(source_lang: str | None, default: str = EN_CA) -> list[str]:
    """One source language code -> a single-element BCP 47 locale list.

    Returns a list because `attributes.locale` is always a list, even for a
    monolingual document; a genuinely bilingual document is tagged by its
    crawler with the full set (e.g. [EN_CA, FR_CA]).
    """
    tag = _SOURCE_TAG.get((source_lang or "").lower(), default)
    return [tag]
