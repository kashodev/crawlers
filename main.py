"""Crawler entrypoint.

Runs one site to completion and exits — the run-to-completion semantics Cloud
Run Jobs expects. Exit code is non-zero when the job failed, so Cloud Run
retries the task and job failures are visible in the console.

    python main.py --site toronto_bylaws
    python main.py --site ontario_legislative_bills --max-documents 5
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
from pathlib import Path

from common.env import load_env

load_env()  # before anything reads os.environ

from common import config as config_module  # noqa: E402
from common.crawler import BaseCrawler  # noqa: E402
from common.models import JobStatus  # noqa: E402
from utils.logging_config import configure_logging  # noqa: E402

ROOT = Path(__file__).resolve().parent
log = logging.getLogger("main")


def available_sites() -> list[str]:
    return sorted(
        child.name
        for child in ROOT.iterdir()
        if child.is_dir() and (child / "config.yaml").exists() and (child / "crawler.py").exists()
    )


def load_crawler_class(module_name: str) -> type[BaseCrawler]:
    module = importlib.import_module(f"{module_name}.crawler")
    crawler_cls = getattr(module, "CRAWLER", None)
    if crawler_cls is None or not issubclass(crawler_cls, BaseCrawler):
        raise AttributeError(f"{module_name}.crawler must export CRAWLER (a BaseCrawler subclass)")
    return crawler_cls


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download public-sector PDFs for RAG test data.")
    parser.add_argument("--site", help="site module to crawl (default: $CRAWL_SITE)")
    parser.add_argument("--config", help="override config path or gs:// URI")
    parser.add_argument("--max-documents", type=int, help="stop after N downloads")
    parser.add_argument("--min-delay", type=float, help="override seconds between requests")
    parser.add_argument("--job-id", help="reuse a job id (e.g. to group task shards)")
    parser.add_argument("--log-level", default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="discover and log candidates without downloading or writing metadata",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="ignore the saved checkpoint and crawl the range from the top",
    )
    parser.add_argument(
        "--show-checkpoint",
        action="store_true",
        help="print the saved resume position for the site and exit",
    )
    parser.add_argument("--list-sites", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    if args.list_sites:
        print("\n".join(available_sites()))
        return 0

    site = args.site or os.environ.get("CRAWL_SITE")
    if not site:
        log.error("No site selected. Use --site or set CRAWL_SITE.", extra={"available": available_sites()})
        return 2
    if site not in available_sites():
        log.error("Unknown site", extra={"site": site, "available": available_sites()})
        return 2

    config_path = args.config or os.environ.get("CONFIG_URI") or str(ROOT / site / "config.yaml")
    cfg = config_module.load(config_path)

    if args.show_checkpoint:
        import json

        from common.metadata import build_repository

        checkpoint = build_repository(cfg.metadata).get_checkpoint(cfg.name)
        print(json.dumps(checkpoint, indent=2) if checkpoint else "no checkpoint saved")
        return 0

    # Read by BaseCrawler.load_resume_state; also settable as an env var so a
    # Cloud Run job can force a full re-crawl without changing its args.
    if args.no_resume:
        os.environ["CRAWLER_NO_RESUME"] = "1"

    if args.max_documents is not None:
        cfg.limits.max_documents = args.max_documents
    if args.min_delay is not None:
        cfg.politeness.min_delay_seconds = args.min_delay

    crawler = load_crawler_class(cfg.module)(cfg, job_id=args.job_id)

    if args.dry_run:
        log.info("Dry run: discovering candidates only", extra={"site": cfg.name})
        for count, candidate in enumerate(crawler.discover(), start=1):
            log.info(
                "Candidate",
                extra={"url": candidate.url, "key": candidate.storage_key, "n": count},
            )
            if cfg.limits.max_documents and count >= cfg.limits.max_documents:
                break
        crawler.client.close()
        return 0

    job = crawler.run()
    # STOPPED_EARLY is a normal outcome (miss streak / document cap), so only a
    # genuine FAILED status should fail the container.
    return 1 if job.status == JobStatus.FAILED.value else 0


if __name__ == "__main__":
    sys.exit(main())
