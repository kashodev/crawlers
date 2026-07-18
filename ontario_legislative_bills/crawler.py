"""Ontario Legislative Assembly bill crawler.

Site shape (server-rendered Drupal, no JS required):

  /en/legislative-business/bills/all
      -> links to every parliament/session
  /en/legislative-business/bills/parliament-44/session-1
      -> one table row per bill; the whole session is on one page (no pagination)
  /en/legislative-business/bills/parliament-44/session-1/bill-2
      -> zero or more PDF links, one per legislative stage, e.g.
         .../pdf/2025/2025-04/b002_e.pdf     (First Reading)
         .../pdf/2025/2025-06/b002ra_e.pdf   (Royal Assent)

A bill can carry several PDFs; each is stored as its own document so a RAG
pipeline can distinguish the first-reading text from the enacted version.
Traversal is newest-first: highest parliament, then highest session, then
highest bill number.
"""

from __future__ import annotations

import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from common.crawler import BaseCrawler, PdfCandidate
from common.http import FetchError, RobotsDisallowed
from utils.urls import filename_of, normalize

log = logging.getLogger(__name__)

SESSION_RE = re.compile(r"/bills/parliament-(\d+)/session-(\d+)/?$")
BILL_RE = re.compile(r"/bills/parliament-(\d+)/session-(\d+)/bill-([\w-]+)$")
# Bill PDFs live under a dated path; the filename encodes number + stage + lang.
PDF_NAME_RE = re.compile(r"^b(\d+)([a-z]*)_([ef])\.pdf$", re.IGNORECASE)

# Filename infix -> legislative stage. Useful as RAG metadata and for dedup.
STAGE_CODES = {
    "": "first_reading",
    "ra": "royal_assent",
    "r": "reprint",
    "e": "enacted",
}


class OntarioLegislativeBillsCrawler(BaseCrawler):
    @property
    def base_url(self) -> str:
        return self.config.options.get("base_url", "https://www.ola.org")

    @property
    def languages(self) -> list[str]:
        return [lang.lower() for lang in self.config.options.get("languages") or ["e"]]

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        """Fetch and parse a page, converting expected failures into a None."""
        try:
            resp = self.client.get(url)
        except RobotsDisallowed:
            log.warning("Skipping robots-disallowed page", extra={"url": url})
            return None
        except FetchError as exc:
            log.error("Failed to fetch page", extra={"url": url, "error": str(exc)})
            return None
        return BeautifulSoup(resp.content, "lxml")

    def find_sessions(self) -> list[tuple[int, int, str]]:
        """Return (parliament, session, url), newest first."""
        sessions: dict[tuple[int, int], str] = {}
        wanted = {int(p) for p in (self.config.options.get("parliaments") or [])}

        for seed in self.config.seeds:
            soup = self._get_soup(seed)
            if soup is None:
                continue
            for anchor in soup.find_all("a", href=True):
                match = SESSION_RE.search(anchor["href"])
                if not match:
                    continue
                parliament, session = int(match.group(1)), int(match.group(2))
                if wanted and parliament not in wanted:
                    continue
                sessions[(parliament, session)] = normalize(anchor["href"], seed)

        ordered = sorted(sessions.items(), key=lambda item: item[0], reverse=True)
        log.info(
            "Discovered sessions",
            extra={"count": len(ordered), "newest": ordered[0][0] if ordered else None},
        )
        return [(p, s, url) for (p, s), url in ordered]

    def find_bills(self, session_url: str) -> list[tuple[str, str, str]]:
        """Return (bill_number, title, url) for a session, highest number first."""
        soup = self._get_soup(session_url)
        if soup is None:
            return []

        bills: dict[str, tuple[str, str]] = {}
        for anchor in soup.find_all("a", href=True):
            match = BILL_RE.search(anchor["href"])
            if not match:
                continue
            number = match.group(3)
            bills[number] = (
                anchor.get_text(strip=True),
                normalize(anchor["href"], session_url),
            )

        def sort_key(number: str):
            # Bill numbers are usually integers but can carry a letter suffix
            # (e.g. "5a" for a substituted bill), so sort numerically then
            # lexically rather than assuming int().
            digits = re.match(r"(\d+)(.*)", number)
            return (int(digits.group(1)), digits.group(2)) if digits else (0, number)

        ordered = sorted(bills.items(), key=lambda item: sort_key(item[0]), reverse=True)
        log.info("Discovered bills in session", extra={"url": session_url, "count": len(ordered)})
        return [(number, title, url) for number, (title, url) in ordered]

    def _extract_status(self, soup: BeautifulSoup) -> str | None:
        node = soup.select_one(".views-field-field-current-status-1 .field-content")
        if node is None:
            return None
        text = node.get_text(" ", strip=True)
        return re.sub(r"^Current status:\s*", "", text) or None

    def find_bill_pdfs(
        self, bill_url: str, parliament: int, session: int, number: str, title: str
    ) -> list[PdfCandidate]:
        soup = self._get_soup(bill_url)
        if soup is None:
            return []

        status = self._extract_status(soup)
        candidates: list[PdfCandidate] = []
        seen: set[str] = set()

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if not href.lower().endswith(".pdf"):
                continue

            url = normalize(href, bill_url)
            if url in seen:
                continue
            seen.add(url)

            name = filename_of(url)
            match = PDF_NAME_RE.match(name)
            if match:
                stage_code, language = match.group(2).lower(), match.group(3).lower()
            else:
                # Unrecognized naming: keep the PDF but don't invent metadata.
                stage_code, language = "", "e"
                log.debug("Unrecognized PDF filename", extra={"url": url, "name": name})

            if language not in self.languages:
                continue

            stage = STAGE_CODES.get(stage_code, stage_code or "unknown")
            # Link text is the human label, e.g. "Bill 2 Royal Assent (PDF)".
            label = anchor.get_text(" ", strip=True) or None

            candidates.append(
                PdfCandidate(
                    url=url,
                    storage_key=(
                        f"parliament-{parliament}/session-{session}/bill-{number}/{name}"
                    ),
                    referrer_url=bill_url,
                    attributes={
                        "jurisdiction": "ontario",
                        "documentType": "bill",
                        "parliament": parliament,
                        "session": session,
                        "billNumber": number,
                        "title": title,
                        "status": status,
                        "stage": stage,
                        "language": language,
                        "linkLabel": label,
                        "fileName": name,
                        "billUrl": bill_url,
                    },
                )
            )

        if not candidates:
            log.debug("Bill has no PDFs", extra={"url": bill_url, "bill": number})
        return candidates

    def discover(self) -> Iterator[PdfCandidate]:
        for parliament, session, session_url in self.find_sessions():
            bills = self.find_bills(session_url)
            if not bills:
                # An empty session page is a structural miss worth counting.
                self.record_miss()
                continue

            for number, title, bill_url in bills:
                # Shard on the bill so all of one bill's PDFs land in one task.
                if not self.owns(f"{parliament}-{session}-{number}"):
                    continue

                pdfs = self.find_bill_pdfs(bill_url, parliament, session, number, title)
                if not pdfs:
                    self.record_miss()
                    continue

                self.record_hit()
                yield from pdfs


CRAWLER = OntarioLegislativeBillsCrawler
