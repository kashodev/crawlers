"""URL normalization helpers."""

from __future__ import annotations

from urllib.parse import urldefrag, urljoin, urlparse, urlunparse


def normalize(url: str, base: str | None = None) -> str:
    """Absolutize and canonicalize a URL so the same resource hashes identically."""
    if base:
        url = urljoin(base, url)
    url, _ = urldefrag(url)
    parts = urlparse(url)
    netloc = parts.netloc.lower()
    if (parts.scheme == "https" and netloc.endswith(":443")) or (
        parts.scheme == "http" and netloc.endswith(":80")
    ):
        netloc = netloc.rsplit(":", 1)[0]
    path = parts.path or "/"
    return urlunparse((parts.scheme.lower(), netloc, path, parts.params, parts.query, ""))


def host_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def is_pdf_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def filename_of(url: str) -> str:
    return urlparse(url).path.rsplit("/", 1)[-1]
