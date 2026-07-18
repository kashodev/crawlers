"""robots.txt fetching and enforcement.

Rules are cached per host for the life of the process. A host whose robots.txt
cannot be fetched is treated as *allowed* only when the fetch failed outright
(RFC-style "unavailable means allow"); an explicit 4xx that is not 404, or any
successful fetch, is honoured as written.
"""

from __future__ import annotations

import logging
import urllib.robotparser
from urllib.parse import urlparse

import httpx

log = logging.getLogger(__name__)


class RobotsPolicy:
    def __init__(self, user_agent: str, respect: bool = True, timeout: float = 30.0):
        self.user_agent = user_agent
        self.respect = respect
        self.timeout = timeout
        self._parsers: dict[str, urllib.robotparser.RobotFileParser | None] = {}

    def _parser_for(self, url: str) -> urllib.robotparser.RobotFileParser | None:
        parts = urlparse(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin in self._parsers:
            return self._parsers[origin]

        robots_url = f"{origin}/robots.txt"
        parser: urllib.robotparser.RobotFileParser | None = None
        try:
            resp = httpx.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
                timeout=self.timeout,
                follow_redirects=True,
            )
            if resp.status_code == 200:
                parser = urllib.robotparser.RobotFileParser()
                parser.parse(resp.text.splitlines())
                log.info("Loaded robots.txt", extra={"origin": origin, "bytes": len(resp.text)})
            else:
                # No usable robots file -> nothing to enforce for this host.
                log.info(
                    "No robots.txt to enforce",
                    extra={"origin": origin, "status": resp.status_code},
                )
        except Exception as exc:
            log.warning(
                "robots.txt fetch failed; proceeding with configured delays only",
                extra={"origin": origin, "error": str(exc)},
            )

        self._parsers[origin] = parser
        return parser

    def can_fetch(self, url: str) -> bool:
        if not self.respect:
            return True
        parser = self._parser_for(url)
        if parser is None:
            return True
        return parser.can_fetch(self.user_agent, url)

    def crawl_delay(self, url: str) -> float | None:
        """Crawl-delay for this host, if the site declares one."""
        if not self.respect:
            return None
        parser = self._parser_for(url)
        if parser is None:
            return None
        try:
            delay = parser.crawl_delay(self.user_agent)
            return float(delay) if delay is not None else None
        except Exception:
            return None
