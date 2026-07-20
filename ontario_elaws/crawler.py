"""Ontario e-Laws crawler — current consolidated statutes and regulations.

Enumeration uses the site's own JSON API (reverse-engineered from the React
bundle and confirmed live), not HTML scraping:

    GET /laws/api/v2/legislation/en/browse-search
        ?text=&selection=current&result=statute&result=regulation
        &page=N&pageSize=100&sort=Newest

Each hit's `_source` carries everything needed to enumerate without fetching the
law: `code` (numeric id), `alias` ("statute/90h08" -> page path), `volume`
(citation), `act` (parent act), `title`, `state`, `dateFrom`.

The law itself is available only as a web page or Word file; there is no PDF.
The Word download is JavaScript-gated, but the page is server-side prerendered
for non-browser User-Agents (our honest crawler UA included) — the full
legislative text sits in a `.WordSection1` container. We fetch that HTML and
render it to PDF with WeasyPrint (see common/pdf.py).

  IMPORTANT: a browser-like User-Agent gets a JavaScript shell instead of the
  prerendered HTML. The default crawler UA must stay non-browser.

Consolidated laws are re-published in place as they are amended, unlike bylaws
and bills. So the "new documents" pass does two things: pick up newly enacted
laws (above the frontier) and re-render laws whose `last_amendment` advanced
since we stored them (see reprocess_reason).

Repealed/revoked/spent laws (the API's `selection=rrs`) are out of scope for
now; `status` is recorded so they can be added later without a reshape.
"""

from __future__ import annotations

import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from common.crawler import BaseCrawler, PdfCandidate
from common.http import FetchError
from common.models import DocumentRecord
from common.pdf import PdfRenderError, render_law_pdf

log = logging.getLogger(__name__)

API_BASE = "https://www.ontario.ca/laws/api/v2/legislation"
# Any id segment works; the endpoint returns the single global currency date.
CURRENCY_URL = f"{API_BASE}/statutes/currency-date"


def _amend_scope(flat: str) -> int:
    """Upper bound for the 'No amendments' check: only the header region.

    "No amendments" must describe this law's own header, not appear somewhere
    deep in its text, so limit the search to just past the Last-amendment line.
    """
    i = flat.find("Last amendment:")
    if i == -1:
        i = flat.find("Consolidation Period")
    return (i + 200) if i != -1 else min(len(flat), 4000)


def _en(value) -> str | None:
    """Unwrap the API's {"en": "..."} localized fields."""
    if isinstance(value, dict):
        return value.get("en")
    return value


class OntarioELawsCrawler(BaseCrawler):
    # ---- config accessors -------------------------------------------------

    @property
    def result_types(self) -> list[str]:
        return list(self.config.options.get("result_types") or ["statute", "regulation"])

    @property
    def page_size(self) -> int:
        return int(self.config.options.get("api_page_size") or 100)

    @property
    def selection(self) -> str:
        # "current" = consolidated in force. "rrs" (repealed/revoked/spent) is
        # deferred; exposed in config so it can be enabled later.
        return self.config.options.get("selection") or "current"

    # ---- currency date (global, cached for the run) -----------------------

    _currency_date: str | None = None

    def currency_date(self) -> str | None:
        if self._currency_date is None:
            try:
                resp = self.client.get(CURRENCY_URL)
                self._currency_date = resp.text.strip() or None
                log.info("e-Laws currency date", extra={"currencyDate": self._currency_date})
            except FetchError as exc:
                log.warning("Could not fetch currency date", extra={"error": str(exc)})
                self._currency_date = None
        return self._currency_date

    # ---- enumeration ------------------------------------------------------

    def _browse_page(self, result_type: str, page: int) -> tuple[list[dict], int]:
        """One page of browse-search results. Returns (hits, total)."""
        url = (
            f"{API_BASE}/en/browse-search"
            f"?text=&selection={self.selection}&result={result_type}"
            f"&page={page}&pageSize={self.page_size}&sort=Newest"
        )
        resp = self.client.get(url)
        try:
            data = resp.content and __import__("json").loads(resp.content)
        except ValueError as exc:
            raise FetchError(f"browse-search returned non-JSON for {url}: {exc}")
        hits = (data.get("hits") or {}).get("hits") or []
        total = ((data.get("hits") or {}).get("total") or {}).get("value") or 0
        return hits, total

    def enumerate_laws(self) -> Iterator[dict]:
        """Yield every current law as a normalized dict, newest first.

        Enumeration is cheap (one request per `page_size` laws), so every task
        enumerates the full list and filters to its shard in discover() — no
        need to shard the enumeration itself.
        """
        for result_type in self.result_types:
            page = 1
            seen = 0
            while True:
                hits, total = self._browse_page(result_type, page)
                if not hits:
                    break
                for hit in hits:
                    parsed = self._parse_hit(hit, result_type)
                    if parsed:
                        yield parsed
                seen += len(hits)
                log.info(
                    "Enumerated page",
                    extra={"type": result_type, "page": page, "seen": seen, "total": total},
                )
                if seen >= total:
                    break
                page += 1

    def _parse_hit(self, hit: dict, result_type: str) -> dict | None:
        src = hit.get("_source") or {}
        alias = _en(src.get("alias"))  # e.g. "statute/90h08"
        code = src.get("code")
        if not alias or code is None:
            log.warning("Skipping hit with no alias/code", extra={"hit": str(hit)[:120]})
            return None
        return {
            "type": result_type,
            # Codes are alphanumeric for statutes ("94c25") and numeric for
            # regulations ("900056"); always a string.
            "code": str(code),
            "alias": alias,
            "citation": _en(src.get("volume")),
            "parent_act": _en(src.get("act")),
            "title": _en(src.get("title")),
            "state": _en(src.get("state")),
            "date_from": _en(src.get("dateFrom")),
            # Elasticsearch sort value: [epoch_ms, year, seq] — a ready-made
            # monotonic ordering for the resume cursor.
            "sort": hit.get("sort") or [],
        }

    # ---- position / sharding / resume hooks -------------------------------

    def _candidate(self, law: dict) -> PdfCandidate:
        alias = law["alias"]
        page_url = f"https://www.ontario.ca/laws/{alias}"
        return PdfCandidate(
            url=page_url,
            storage_key=f"{alias}.pdf",  # statute/90h08.pdf, regulation/900056.pdf
            referrer_url="https://www.ontario.ca/laws/search",
            attributes={
                "jurisdiction": "ontario",
                "documentType": law["type"],
                "lawCode": law["code"],
                "alias": alias,
                "citation": law.get("citation"),
                "parentAct": law.get("parent_act"),
                "title": law.get("title"),
                # Reserved for repealed/revoked work; "current" for now.
                "status": law.get("state") or "current",
                "consolidationDateFrom": law.get("date_from"),
                # Filled in during process() from the page + global endpoint.
                "lastAmendment": None,
                "currencyDate": None,
                "_sort": law.get("sort"),
            },
        )

    def position_of(self, candidate: PdfCandidate) -> dict | None:
        attrs = candidate.attributes
        if "lawCode" not in attrs:
            return None
        return {
            "type": attrs["documentType"],
            "code": str(attrs["lawCode"]),
            "sort": attrs.get("_sort") or [],
        }

    def position_rank(self, position: dict) -> tuple:
        # Newest first: order by the API sort value, then code as a tiebreak.
        # Coerce sort elements to a uniform type so tuples always compare.
        sort = tuple(str(x) for x in (position.get("sort") or []))
        return (sort, str(position.get("code", "")))

    def position_sort_key(self, position: dict) -> str:
        sort = position.get("sort") or []
        epoch = int(sort[0]) if sort and str(sort[0]).lstrip("-").isdigit() else 0
        # Zero-padded epoch so string order matches numeric order in Firestore;
        # code is an opaque tiebreak.
        return f"{epoch:016d}-{position.get('code', '')}"

    def shard_key_of(self, position: dict) -> str | None:
        # Shard on the law code string; shard_index() digests it, so alphanumeric
        # statute codes and numeric regulation codes both distribute evenly.
        code = position.get("code")
        return str(code) if code is not None else None

    def position_from_key(self, key: str) -> dict | None:
        # key like "statute/90h08.pdf" or "regulation/900056.pdf". The alias
        # code is exactly the law code, so recovery is complete (minus the sort
        # value, which the checkpoint/Firestore cursor carries).
        match = re.match(r"^(statute|regulation)/([a-z0-9]+)\.pdf$", key)
        if not match:
            return None
        return {"type": match.group(1), "code": match.group(2), "sort": []}

    def owns_law(self, law: dict) -> bool:
        return self.owns(str(law["code"]))

    # ---- re-consolidation: re-render only when a law changed --------------

    def reprocess_reason(self, existing: dict, candidate: PdfCandidate) -> str | None:
        """Re-render a stored law when its amendment or currency date advanced.

        Consolidated laws are re-published in place. We can't know a law changed
        without reading its page, but the cheap signal is the currency date: it
        is global, so if it hasn't moved since we stored the law, nothing has
        been re-consolidated and we can skip. When it has moved, the page fetch
        in produce_document re-reads `last_amendment`; if that is unchanged the
        content is identical and the re-render is harmless (idempotent bytes).
        """
        stored_currency = (existing.get("attributes") or {}).get("currencyDate")
        current = self.currency_date()
        if current and stored_currency and current != stored_currency:
            return f"currency date advanced {stored_currency} -> {current}"
        return None

    # ---- fetch + render ---------------------------------------------------

    def produce_document(self, candidate: PdfCandidate, record: DocumentRecord) -> bytes:
        resp = self.client.get(candidate.url)
        record.http_status = resp.status_code
        html = resp.text

        self._enrich_from_page(record, candidate, html)

        try:
            provenance = self._provenance_line(candidate, record)
            return render_law_pdf(html, base_url=candidate.url, provenance=provenance)
        except PdfRenderError as exc:
            # A render failure is a real failure for this document, surfaced so
            # the job records it and Cloud Run can retry.
            raise FetchError(f"render failed for {candidate.url}: {exc}") from exc

    def _enrich_from_page(self, record: DocumentRecord, candidate: PdfCandidate, html: str) -> None:
        """Pull the per-law dates off the page and stamp them on the record."""
        attrs = record.attributes

        og = re.search(r'og:description"\s+content="([^"]+)"', html)
        blurb = og.group(1) if og else ""

        # The og:description truncates ("...") so parse "Last amendment: …" from
        # the page body, where it runs to a full stop. Strip inline tags first.
        flat = re.sub(r"<[^>]+>", " ", html)
        flat = re.sub(r"\s+", " ", flat)
        # Citations carry internal periods ("c. 24, Sched. 8"), so stop at the
        # section that always follows — "Legislative History" — not the first dot.
        amend = re.search(
            r"Last amendment:\s*(.+?)\s*\.?\s*(?:Legislative History|CONTENTS|Note:|$)", flat
        )
        if re.search(r"No amendments", flat[:_amend_scope(flat)]):
            attrs["lastAmendment"] = "none"
        elif amend and amend.group(1).strip():
            attrs["lastAmendment"] = amend.group(1).strip().rstrip(".")

        consol = re.search(
            r"Consolidation Period:\s*(?:From\s+)?([A-Z][a-z]+ \d{1,2}, \d{4})", blurb or flat
        )
        if consol:
            attrs["consolidationDateFrom"] = consol.group(1)

        attrs["currencyDate"] = self.currency_date()

    def _provenance_line(self, candidate: PdfCandidate, record: DocumentRecord) -> str:
        attrs = record.attributes
        bits = [attrs.get("citation") or attrs.get("title") or candidate.url, candidate.url]
        if attrs.get("currencyDate"):
            bits.append(f"e-Laws currency date: {attrs['currencyDate']}")
        if attrs.get("lastAmendment"):
            bits.append(f"Last amendment: {attrs['lastAmendment']}")
        return " · ".join(b for b in bits if b)

    # ---- two-phase discovery ---------------------------------------------

    def discover(self) -> Iterator[PdfCandidate]:
        state = self.resume
        cursor_rank = self.position_rank(state.cursor) if state.cursor else None
        frontier_rank = self.position_rank(state.frontier) if state.frontier else None

        if state.is_resume:
            log.info("Resuming e-Laws sweep", extra={"cursor": state.cursor})
        elif state.sweep_complete:
            log.info("Sweep complete; checking for new and amended laws only")
        else:
            log.info("Starting full e-Laws sweep")

        for law in self.enumerate_laws():
            if not self.owns_law(law):
                continue
            rank = self.position_rank(self.position_of(self._candidate(law)))

            if not state.sweep_complete:
                # Sweep phase: skip laws at/above the cursor (already swept).
                if cursor_rank is not None and rank >= cursor_rank:
                    continue
                yield self._candidate(law)
            else:
                # Post-sweep: newly enacted laws (above the frontier) plus
                # re-consolidation of changed ones (reprocess_reason decides).
                if frontier_rank is not None and rank > frontier_rank:
                    self.job.new_documents += 1
                yield self._candidate(law)

        if not state.sweep_complete:
            self.mark_sweep_complete()


CRAWLER = OntarioELawsCrawler
