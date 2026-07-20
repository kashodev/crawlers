"""HTML-to-PDF rendering for laws served only as web pages.

The e-Laws pages are Word-exported HTML: the whole legislative document lives in
a single `.WordSection1` container with its own inline styling, wrapped in
Ontario.ca site chrome (header, nav, footer). We render the document container
only — faithful legal text and structure, without the surrounding website —
using WeasyPrint, which is pure-Python and needs no headless browser.

The document's own CONTENTS table (a printed statute's table of contents) is
part of `.WordSection1` and is kept. The site's interactive ToC sidebar is
browser-rendered JavaScript and never appears in the server HTML we fetch, so
there is nothing extra to strip for it.
"""

from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

# Where the legislative document lives. Longer laws are Word-exported into a
# `.WordSection1` container; short regulations skip that wrapper and place their
# paragraphs (regtitle-e, ConsolidationPeriod-e, …) directly in the main
# `.ontario-row` content region. Prefer the tight container, fall back to the row.
CONTENT_SELECTORS = ["WordSection1", "ontario-row"]

# Site chrome and non-document nodes to drop if any leak into the content.
_STRIP_SELECTORS = [
    ("class", "ontario-header"),
    ("class", "ontario-navigation"),
    ("class", "ontario-footer"),
    ("class", "skip-link"),
    ("class", "breadcrumb"),
    ("class", "back-to-top"),
    ("id", "feedback"),
]

# Minimal print styling. The Word HTML carries its own inline styles, so this
# only sets page geometry and a readable default; it never restyles the law.
_PRINT_CSS = """
@page { size: Letter; margin: 2cm 1.8cm; }
body { font-family: Georgia, 'Times New Roman', serif; font-size: 10.5pt;
       line-height: 1.35; color: #111; }
table { border-collapse: collapse; max-width: 100%; }
a { color: #111; text-decoration: none; }
h1, h2 { font-size: 13pt; }
.pdf-provenance { color: #666; font-size: 8.5pt; border-bottom: 1px solid #ccc;
                  margin-bottom: 1em; padding-bottom: 0.5em; }
"""


class PdfRenderError(Exception):
    """Raised when the page can't be turned into a PDF (no content, render failure)."""


def extract_document(html: str):
    """Return the BeautifulSoup node for the legislative document, chrome removed.

    Raises PdfRenderError if the content container is absent — which usually
    means the page was the JavaScript shell (served to browser User-Agents)
    rather than the prerendered HTML, and is a real failure, not an empty law.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    content = None
    for selector in CONTENT_SELECTORS:
        content = soup.find(class_=selector)
        if content is not None:
            break
    if content is None:
        raise PdfRenderError(
            f"no {CONTENT_SELECTORS} container in page; "
            "likely the JS shell, not prerendered HTML"
        )

    for attr, value in _STRIP_SELECTORS:
        for node in content.find_all(attrs={attr: re.compile(value)}):
            node.decompose()
    for tag in content.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    return content


def render_law_pdf(
    html: str,
    *,
    base_url: str,
    provenance: str | None = None,
) -> bytes:
    """Render a law page's document container to PDF bytes.

    `base_url` lets the renderer resolve any relative asset URLs. `provenance`
    is a short line (source URL, currency date) stamped at the top so the stored
    PDF is self-describing for a RAG pipeline.
    """
    from weasyprint import CSS, HTML

    document = extract_document(html)

    banner = ""
    if provenance:
        # Escaped by BeautifulSoup on the way in; safe to inline.
        banner = f'<div class="pdf-provenance">{provenance}</div>'

    page_html = (
        f"<!doctype html><html><head><meta charset='utf-8'></head>"
        f"<body>{banner}{document}</body></html>"
    )

    try:
        pdf = HTML(string=page_html, base_url=base_url).write_pdf(
            stylesheets=[CSS(string=_PRINT_CSS)]
        )
    except Exception as exc:  # WeasyPrint raises a variety of internal errors
        raise PdfRenderError(f"WeasyPrint failed: {type(exc).__name__}: {exc}") from exc

    if not pdf or not pdf.startswith(b"%PDF"):
        raise PdfRenderError("renderer produced no PDF output")
    return pdf
