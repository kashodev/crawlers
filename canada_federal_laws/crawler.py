"""Canada federal law crawler — consolidated Acts and Regulations.

Source: laws-lois.justice.gc.ca. Both the Acts and Regulations sites expose an
alphabetical index — `/eng/acts/{A..Z}.html`, `/eng/regulations/{A..Z}.html` —
where every letter page lists its laws (current *and* repealed) as server-
rendered `<li>` entries. Each entry carries:

    <li><span class="objTitle"><a class="TocTitle" href="{ID}/index.html">
          Title                                   (current)
          <span class='Repealed'>Title [Repealed, 1997, c. 20, s. 45]</span>
        </a></span>
        ...<span class="pdfLink"><a href="/PDF/{ID}.pdf">PDF</a>...

So enumeration is: fetch a letter page, parse each `<li>` for title, id, PDF
href, and status (the `Repealed` marker, kept verbatim as the site labels it —
acts and regulations alike say "repealed"). Repealed laws are included, as
requested, and marked.

The PDF is a real file at `/PDF/{ID}.pdf` (no rendering needed). For every
downloaded law we also fetch its detail page (`{ID}/index.html`) for the
consolidation metadata the index page lacks — "current to" / "last amended"
dates, and the enabling act for regulations.

Federal PDFs are re-consolidated over time. `reprocess_reason` (off unless
`reprocess_on_change` is set) HEAD-probes the PDF and re-downloads when its
ETag / Last-Modified has moved since we stored it.

Sharding is by (type, letter): a task owns whole letter pages, so parallel
tasks never re-fetch or double-download. Order is alphabetical; the sweep runs
in descending rank so the base cursor/frontier resume works unchanged.
"""

from __future__ import annotations

import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from common.crawler import BaseCrawler, PdfCandidate
from common.http import FetchError, RobotsDisallowed
from common.models import DocumentRecord
from utils.locale import to_locale
from utils.urls import normalize

log = logging.getLogger(__name__)

BASE = "https://laws-lois.justice.gc.ca"
LETTERS = [chr(c) for c in range(ord("A"), ord("Z") + 1)]

# Acts sweep before regulations; encoded so descending rank crawls acts first.
_TYPE_ORDER = {"act": 1, "regulation": 0}
_TYPE_PATH = {"act": "acts", "regulation": "regulations"}

_PDF_ID_RE = re.compile(r"/PDF/(.+)\.pdf$", re.IGNORECASE)
_REPEAL_RE = re.compile(r"\[(Repealed|Revoked)[^\]]*\]", re.IGNORECASE)
# storage_key layout: {type}/{letter}/{id}.pdf — letter kept so a stored object
# alone identifies its shard, which the checkpoint-less recovery path needs.
_KEY_RE = re.compile(r"^(acts|regulations)/([A-Z])/(.+)\.pdf$")

_PATH_TYPE = {"acts": "act", "regulations": "regulation"}


def _letter_order(letter: str) -> int:
    # A -> 26 (highest), Z -> 1, so iterating A..Z is descending rank.
    return 26 - (ord(letter.upper()) - ord("A"))


# Module-level so migrations/tooling can map a storage key to a position without
# constructing a crawler (mirrors the other site modules).
def position_from_key(key: str) -> dict | None:
    match = _KEY_RE.match(key or "")
    if not match:
        return None
    return {"type": _PATH_TYPE[match.group(1)], "letter": match.group(2), "law_id": match.group(3)}


def position_sort_key(position: dict) -> str:
    # Lexicographic order matches position_rank so the Firestore frontier query
    # (ordered on sort_key) and the in-memory rank agree.
    type_order = _TYPE_ORDER.get(position.get("type"), 0)
    letter_order = _letter_order(position.get("letter", "A"))
    return f"{type_order}|{letter_order:02d}|{position.get('law_id', '')}"


class CanadaFederalLawsCrawler(BaseCrawler):
    # ---- config -----------------------------------------------------------

    @property
    def types(self) -> list[str]:
        return list(self.config.options.get("types") or ["act", "regulation"])

    @property
    def letters(self) -> list[str]:
        configured = self.config.options.get("letters")
        return [c.upper() for c in configured] if configured else LETTERS

    @property
    def reprocess_on_change(self) -> bool:
        return bool(self.config.options.get("reprocess_on_change", False))

    # ---- enumeration ------------------------------------------------------

    def _letter_url(self, doc_type: str, letter: str) -> str:
        return f"{BASE}/eng/{_TYPE_PATH[doc_type]}/{letter}.html"

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        try:
            resp = self.client.get(url, allow_status=(404,))
        except RobotsDisallowed:
            log.warning("robots-disallowed page", extra={"url": url})
            return None
        except FetchError as exc:
            log.error("Failed to fetch page", extra={"url": url, "error": str(exc)})
            return None
        if resp.status_code == 404:
            log.info("No such letter page", extra={"url": url})
            return None
        return BeautifulSoup(resp.content, "lxml")

    def parse_letter(self, doc_type: str, letter: str) -> list[dict]:
        """Parse one letter page into law dicts, sorted descending by rank."""
        soup = self._get_soup(self._letter_url(doc_type, letter))
        if soup is None:
            return []

        laws: dict[str, dict] = {}
        for li in soup.select("ul.titlesList > li"):
            pdf_anchor = li.select_one("span.pdfLink a[href]")
            if pdf_anchor is None:
                continue  # entries without a PDF (rare) are not downloadable
            pdf_href = pdf_anchor["href"]
            id_match = _PDF_ID_RE.search(pdf_href)
            if not id_match:
                continue
            law_id = id_match.group(1)

            title_anchor = li.select_one("a.TocTitle")
            raw_title = title_anchor.get_text(" ", strip=True) if title_anchor else ""
            repealed_span = li.select_one("span.Repealed")
            status = "repealed" if repealed_span is not None else "current"
            status_citation = None
            citation_match = _REPEAL_RE.search(raw_title)
            if citation_match:
                status_citation = citation_match.group(0)
            # Clean title: drop the trailing "[Repealed, ...]" annotation.
            title = _REPEAL_RE.sub("", raw_title).strip()

            detail_anchor = li.select_one('a.TocTitle[href]')
            detail_url = (
                normalize(detail_anchor["href"], self._letter_url(doc_type, letter))
                if detail_anchor
                else None
            )

            laws[law_id] = {
                "type": doc_type,
                "letter": letter,
                "law_id": law_id,
                "title": title,
                "status": status,
                "status_citation": status_citation,
                "pdf_url": normalize(pdf_href, BASE),
                "detail_url": detail_url,
            }

        # Descending id within the letter so the whole sweep is descending rank.
        ordered = sorted(laws.values(), key=lambda law: law["law_id"], reverse=True)
        log.info(
            "Parsed letter page",
            extra={"type": doc_type, "letter": letter, "count": len(ordered)},
        )
        return ordered

    def enumerate_laws(self) -> Iterator[dict]:
        """Yield owned laws in descending rank order.

        Only letter pages this shard owns are fetched, so parallel tasks do not
        re-parse each other's pages. Types are ordered acts-before-regulations
        and letters A..Z, which is descending rank by construction.
        """
        for doc_type in sorted(self.types, key=lambda t: _TYPE_ORDER.get(t, 0), reverse=True):
            for letter in self.letters:
                if not self.owns(f"{doc_type}:{letter}"):
                    continue
                for law in self.parse_letter(doc_type, letter):
                    yield law

    # ---- candidate + position hooks ---------------------------------------

    def _candidate(self, law: dict) -> PdfCandidate:
        return PdfCandidate(
            url=law["pdf_url"],
            storage_key=f"{_TYPE_PATH[law['type']]}/{law['letter']}/{law['law_id']}.pdf",
            referrer_url=self._letter_url(law["type"], law["letter"]),
            attributes={
                "jurisdiction": "canada-federal",
                "documentType": law["type"],
                "lawId": law["law_id"],
                "title": law["title"],
                # Verbatim site label ("current" / "repealed"), incl. citation.
                "status": law["status"],
                "statusCitation": law.get("status_citation"),
                "locale": to_locale("en"),
                "letter": law["letter"],
                "detailUrl": law.get("detail_url"),
                # Populated in produce_document from the detail page + PDF headers.
                "currentToDate": None,
                "lastAmendedDate": None,
                "enablingAct": None,
                "sourceLastModified": None,
                "sourceEtag": None,
            },
        )

    def position_of(self, candidate: PdfCandidate) -> dict | None:
        attrs = candidate.attributes
        if "lawId" not in attrs:
            return None
        return {"type": attrs["documentType"], "letter": attrs["letter"], "law_id": attrs["lawId"]}

    def position_rank(self, position: dict) -> tuple:
        return (
            _TYPE_ORDER.get(position.get("type"), 0),
            _letter_order(position.get("letter", "A")),
            str(position.get("law_id", "")),
        )

    def position_sort_key(self, position: dict) -> str:
        return position_sort_key(position)

    def shard_key_of(self, position: dict) -> str | None:
        letter = position.get("letter")
        doc_type = position.get("type")
        return f"{doc_type}:{letter}" if letter and doc_type else None

    def position_from_key(self, key: str) -> dict | None:
        return position_from_key(key)

    # ---- fetch: PDF + detail page + change-signal headers ------------------

    def produce_document(self, candidate: PdfCandidate, record: DocumentRecord) -> bytes:
        resp = self.client.download(candidate.url, self.config.limits.max_pdf_bytes)
        record.http_status = resp.status_code
        if resp.content_type not in ("application/pdf", "application/octet-stream"):
            if not resp.content.startswith(b"%PDF"):
                raise FetchError(
                    f"expected PDF, got {resp.content_type or 'unknown'}", resp.status_code
                )

        # Change signal for reprocess_reason: capture at download time.
        record.attributes["sourceLastModified"] = resp.headers.get("last-modified")
        record.attributes["sourceEtag"] = resp.headers.get("etag")

        # Detail-page metadata is always saved (per requirement).
        self._enrich_from_detail(record, candidate)
        return resp.content

    def _enrich_from_detail(self, record: DocumentRecord, candidate: PdfCandidate) -> None:
        detail_url = record.attributes.get("detailUrl")
        if not detail_url:
            return
        soup = self._get_soup(detail_url)
        if soup is None:
            log.warning("Detail page unavailable", extra={"url": detail_url})
            return
        flat = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

        current_to = re.search(r"current to (\d{4}-\d{2}-\d{2})", flat, re.IGNORECASE)
        if current_to:
            record.attributes["currentToDate"] = current_to.group(1)
        amended = re.search(r"last amended on (\d{4}-\d{2}-\d{2})", flat, re.IGNORECASE)
        if amended:
            record.attributes["lastAmendedDate"] = amended.group(1)
        if record.attributes.get("documentType") == "regulation":
            enabling = re.search(r"Enabling Act:\s*([^\n]+?)(?:\s+Notes|\s{2,}|$)", flat)
            if enabling:
                record.attributes["enablingAct"] = enabling.group(1).strip()[:120]

    # ---- change detection -------------------------------------------------

    def reprocess_reason(self, existing: dict, candidate: PdfCandidate) -> str | None:
        """Re-download when the PDF changed, if change-checking is enabled.

        Cheap: a single HEAD (no body) compares the current ETag/Last-Modified
        against what we stored. Off by default so routine runs don't pay a HEAD
        per already-stored law; enable for a periodic freshness sweep.
        """
        if not self.reprocess_on_change:
            return None

        stored = existing.get("attributes") or {}
        stored_etag = stored.get("sourceEtag")
        stored_modified = stored.get("sourceLastModified")
        if not stored_etag and not stored_modified:
            return None  # nothing to compare against; leave it

        found, headers = self.client.exists(candidate.url)
        if not found:
            return None  # gone or unreachable; let a normal run handle it
        etag = headers.get("etag")
        modified = headers.get("last-modified")

        if stored_etag and etag and etag != stored_etag:
            return f"etag changed {stored_etag} -> {etag}"
        if stored_modified and modified and modified != stored_modified:
            return f"last-modified changed {stored_modified} -> {modified}"
        return None

    # ---- two-phase discovery ---------------------------------------------

    def discover(self) -> Iterator[PdfCandidate]:
        state = self.resume
        cursor_rank = self.position_rank(state.cursor) if state.cursor else None
        frontier_rank = self.position_rank(state.frontier) if state.frontier else None

        if state.is_resume:
            log.info("Resuming federal sweep", extra={"cursor": state.cursor})
        elif state.sweep_complete:
            log.info("Sweep complete; checking for new and changed laws only")
        else:
            log.info("Starting full federal sweep")

        for law in self.enumerate_laws():
            rank = self.position_rank(self.position_of(self._candidate(law)))

            if not state.sweep_complete:
                if cursor_rank is not None and rank >= cursor_rank:
                    continue  # already swept
                yield self._candidate(law)
            else:
                if frontier_rank is not None and rank > frontier_rank:
                    self.job.new_documents += 1
                yield self._candidate(law)

        if not state.sweep_complete:
            self.mark_sweep_complete()


CRAWLER = CanadaFederalLawsCrawler
