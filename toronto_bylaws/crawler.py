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

from common.crawler import BaseCrawler, PdfCandidate

log = logging.getLogger(__name__)

# robots.txt disallows /legdocs/mmis/*/bgrd/ and one specific 2002 agenda PDF.
# The bylaw path itself is permitted; PoliteClient re-checks every URL anyway.
DEFAULT_TEMPLATE = "https://www.toronto.ca/legdocs/bylaws/{year}/law{number:04d}.pdf"


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

    def scan_year(self, year: int) -> Iterator[tuple[int, dict]]:
        """Yield (number, headers) for each bylaw PDF in `year`, highest first.

        Streams rather than collecting so a document cap or the global miss
        limit takes effect immediately instead of after a full-year scan.
        Stops descending once `per_unit_miss_limit` consecutive numbers are
        absent, which marks the bottom of that year's numbering.
        """
        highest = self.find_highest_number(year)
        if highest is None:
            return

        gap = 0
        found = 0
        per_unit_limit = self.config.limits.per_unit_miss_limit

        for number in range(highest, 0, -1):
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

    def discover(self) -> Iterator[PdfCandidate]:
        for year in range(self.start_year, self.end_year - 1, -1):
            # Shard by year so a task owns whole years; scanning is the
            # expensive part and this keeps tasks from duplicating probes.
            if not self.owns(year):
                log.debug("Year not owned by this task", extra={"year": year})
                continue

            # scan_year descends, so bylaws arrive most-recent-first already.
            for number, headers in self.scan_year(year):
                url = self.pdf_url(year, number)
                yield PdfCandidate(
                    url=url,
                    storage_key=f"{year}/law{number:04d}.pdf",
                    referrer_url=f"https://www.toronto.ca/legdocs/bylaws/{year}/",
                    attributes={
                        "jurisdiction": "toronto",
                        "documentType": "bylaw",
                        "year": year,
                        "bylawNumber": number,
                        # Canonical citation, e.g. "1234-2024".
                        "bylawId": f"{number}-{year}",
                        "fileName": f"law{number:04d}.pdf",
                        # Recorded at probe time; lets a later run detect that
                        # the source document changed without re-downloading.
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
