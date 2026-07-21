"""City of Toronto bylaw crawler.

Bylaw PDFs are served at a predictable path:

    https://www.toronto.ca/legdocs/bylaws/{year}/law{NNNN}.pdf

so discovery is a HEAD probe over (year, number). A HEAD transfers no body,
which keeps enumeration cheap for the origin as well as for us.

Numbering has holes — not every bylaw has a PDF — so two miss budgets apply:

  * per_unit_miss_limit: consecutive gaps tolerated inside one year before
    concluding the year is exhausted and moving to the previous one.
  * consecutive_miss_limit: consecutive gaps across the whole crawl. Hitting
    it stops the run completely (the 1000-miss rule).

Order is newest-first: years descend, and within a year the discovered bylaws
are emitted highest-number-first (numbers are assigned chronologically).

Why not the Bylaw Status Registry UI: it is a Backbone app whose data APIs
(secure.toronto.ca/BlsrPublicRest, /c3api_data/...) are behind Akamai bot
protection and refuse non-browser clients. This path serves the same PDFs and
is allowed by robots.txt.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Iterator

from common.crawler import BaseCrawler, PdfCandidate  # noqa: F401
from utils.locale import to_locale

log = logging.getLogger(__name__)

# robots.txt disallows /legdocs/mmis/*/bgrd/ and one specific 2002 agenda PDF.
# The bylaw path itself is permitted; PoliteClient re-checks every URL anyway.
DEFAULT_TEMPLATE = "https://www.toronto.ca/legdocs/bylaws/{year}/law{number:04d}.pdf"

_KEY_RE = re.compile(r"^(\d{4})/law(\d+)\.pdf$")


# Module-level so migrations and tooling can map keys to positions without
# constructing a crawler (which would open an HTTP client and storage).
def position_from_key(key: str) -> dict | None:
    match = _KEY_RE.match(key or "")
    if not match:
        return None
    return {"year": int(match.group(1)), "number": int(match.group(2))}


def position_sort_key(position: dict) -> str:
    # Zero-padded so string ordering matches numeric ordering.
    return f"{int(position.get('year', 0)):04d}-{int(position.get('number', 0)):06d}"


class TorontoBylawsCrawler(BaseCrawler):
    @property
    def template(self) -> str:
        return self.config.options.get("pdf_url_template") or DEFAULT_TEMPLATE

    @property
    def start_year(self) -> int:
        configured = self.config.options.get("start_year")
        return int(configured) if configured else datetime.now(timezone.utc).year

    @property
    def end_year(self) -> int:
        return int(self.config.options.get("end_year") or 1998)

    @property
    def max_number(self) -> int:
        return int(self.config.options.get("max_number_per_year") or 2000)

    def pdf_url(self, year: int, number: int) -> str:
        return self.template.format(year=year, number=number)

    def _probe(self, year: int, number: int) -> tuple[bool, dict]:
        url = self.pdf_url(year, number)
        exists, headers = self.client.exists(url)
        log.debug(
            "Probed bylaw", extra={"url": url, "exists": exists, "year": year, "number": number}
        )
        return exists, headers

    def find_highest_number(self, year: int) -> int | None:
        """Find the highest bylaw number in `year` that has a PDF.

        Needed because we emit newest-first and bylaw numbers are chronological,
        so we must know the top of the range before descending. Probing every
        number from max_number downward would waste hundreds of requests on the
        empty tail above the real maximum, so this goes coarse-then-fine:
        descend in strides to bracket the maximum, then walk that stride
        one-by-one to land on it. ~50 probes instead of ~600.
        """
        stride = 32
        bracket_low: int | None = None

        for number in range(self.max_number, 0, -stride):
            exists, _ = self._probe(year, number)
            if exists:
                bracket_low = number
                break

        if bracket_low is None:
            log.info("No bylaws found in year", extra={"year": year})
            return None

        # True maximum lies in (bracket_low, bracket_low + stride].
        highest = bracket_low
        for number in range(bracket_low + 1, min(bracket_low + stride, self.max_number) + 1):
            exists, _ = self._probe(year, number)
            if exists:
                highest = number

        log.info("Located top of year", extra={"year": year, "highestNumber": highest})
        return highest

    def scan_year(
        self,
        year: int,
        start_below: int | None = None,
        stop_below: int = 0,
    ) -> Iterator[tuple[int, dict]]:
        """Yield (number, headers) for each bylaw PDF in `year`, highest first.

        Streams rather than collecting so a document cap or the global miss
        limit takes effect immediately instead of after a full-year scan.
        Stops descending once `per_unit_miss_limit` consecutive numbers are
        absent, which marks the bottom of that year's numbering.

        `start_below` resumes partway down a year (exclusive), skipping the
        already-crawled top. `stop_below` stops at a floor (exclusive), which
        is how the new-document pass scans only above the frontier.
        """
        if start_below is not None:
            # Resuming inside this year: the numbers above are already done, so
            # locating the year's true top would waste ~50 probes.
            highest = start_below - 1
            if highest <= stop_below:
                return
            log.info("Resuming year partway", extra={"year": year, "fromNumber": highest})
        else:
            highest = self.find_highest_number(year)
            if highest is None:
                return

        gap = 0
        found = 0
        per_unit_limit = self.config.limits.per_unit_miss_limit

        for number in range(highest, stop_below, -1):
            exists, headers = self._probe(year, number)

            if exists:
                found += 1
                gap = 0
                self.record_hit()
                yield number, headers
                continue

            gap += 1
            # May raise StopCrawl when the global streak hits the limit.
            self.record_miss()

            if gap >= per_unit_limit:
                log.info(
                    "Year exhausted",
                    extra={"year": year, "found": found, "trailingGap": gap},
                )
                break

        log.info("Scanned year", extra={"year": year, "pdfsFound": found})

    # --- resume support ---------------------------------------------------

    def position_of(self, candidate: PdfCandidate) -> dict | None:
        attrs = candidate.attributes
        if "year" not in attrs or "bylawNumber" not in attrs:
            return None
        return {"year": attrs["year"], "number": attrs["bylawNumber"]}

    def position_rank(self, position: dict) -> tuple:
        # Descending sweep order: newer year first, then higher number.
        return (int(position.get("year", 0)), int(position.get("number", 0)))

    def position_from_key(self, key: str) -> dict | None:
        return position_from_key(key)

    def position_sort_key(self, position: dict) -> str:
        return position_sort_key(position)

    def shard_key_of(self, position: dict) -> int | None:
        # Discovery shards on the year; ownership must be decided the same way.
        year = position.get("year")
        return int(year) if year is not None else None

    def discover(self) -> Iterator[PdfCandidate]:
        state = self.resume

        if state.is_resume:
            # An earlier run died mid-sweep. Continue from just below where it
            # stopped before looking at anything else, so a long backfill is
            # never restarted from the top.
            cursor = state.cursor or {}
            log.info(
                "Resuming interrupted sweep",
                extra={"cursor": cursor, "source": state.source},
            )
            yield from self._sweep(
                from_year=int(cursor.get("year", self.start_year)),
                below_number=int(cursor.get("number", 0)),
            )
        elif not state.sweep_complete:
            log.info("Starting full sweep", extra={"startYear": self.start_year})
            yield from self._sweep(from_year=self.start_year, below_number=None)
        else:
            log.info("Sweep already complete; checking for new bylaws only")

        # The sweep covered the configured range, so anything still missing is
        # material published since the last run — above the frontier.
        self.mark_sweep_complete()
        yield from self._check_for_new(state.frontier)

    def _check_for_new(self, frontier: dict | None) -> Iterator[PdfCandidate]:
        """Look for bylaws newer than anything downloaded so far.

        Only the current year (and any year since the frontier) can gain new
        documents, so this is a small, cheap pass compared to the full sweep.
        """
        if not frontier:
            return

        frontier_year = int(frontier.get("year", 0))
        frontier_number = int(frontier.get("number", 0))
        current_year = datetime.now(timezone.utc).year

        if frontier_year > current_year:
            return

        log.info(
            "Checking for new bylaws",
            extra={"frontier": frontier, "throughYear": current_year},
        )

        for year in range(current_year, frontier_year - 1, -1):
            if not self.owns(year):
                continue
            # Within the frontier's own year, only numbers above it are new.
            floor = frontier_number if year == frontier_year else 0
            for number, headers in self.scan_year(year, stop_below=floor):
                self.job.new_documents += 1
                yield self._candidate(year, number, headers)

    def _sweep(self, from_year: int, below_number: int | None) -> Iterator[PdfCandidate]:
        for year in range(from_year, self.end_year - 1, -1):
            # Shard by year so a task owns whole years; scanning is the
            # expensive part and this keeps tasks from duplicating probes.
            if not self.owns(year):
                log.debug("Year not owned by this task", extra={"year": year})
                continue

            # Only the first year of a resumed sweep starts partway down.
            start_below = below_number if (year == from_year and below_number) else None

            # scan_year descends, so bylaws arrive most-recent-first already.
            for number, headers in self.scan_year(year, start_below=start_below):
                yield self._candidate(year, number, headers)

    def _candidate(self, year: int, number: int, headers: dict) -> PdfCandidate:
        url = self.pdf_url(year, number)
        return PdfCandidate(
            url=url,
            storage_key=f"{year}/law{number:04d}.pdf",
            referrer_url=f"https://www.toronto.ca/legdocs/bylaws/{year}/",
            attributes={
                "jurisdiction": "toronto",
                "documentType": "bylaw",
                # BCP 47 list, consistent with migration 0003. Toronto bylaws
                # are published in English.
                "locale": to_locale("en"),
                "year": year,
                "bylawNumber": number,
                # Canonical citation, e.g. "1234-2024".
                "bylawId": f"{number}-{year}",
                "fileName": f"law{number:04d}.pdf",
                # Recorded at probe time; lets a later run detect that the
                # source document changed without re-downloading.
                "lastModified": headers.get("last-modified"),
                "contentLengthHeader": _int_or_none(headers.get("content-length")),
                "registryUrl": (
                    "https://www.toronto.ca/city-government/public-notices-bylaws/"
                    "bylaws-municipal-code/bylaw-status-registry/"
                    "search-bylaw-status-registry/"
                ),
            },
        )


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(re.sub(r"[^0-9]", "", value) or 0) or None
    except ValueError:
        return None


CRAWLER = TorontoBylawsCrawler
