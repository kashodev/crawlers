# PDF Crawler

Downloads public-sector PDFs (Ontario legislature bills, Toronto bylaws) as RAG
test data. Runs to completion and exits, so it costs nothing between crawls.

## Quick start

```bash
cp .env.example .env          # optional: blank .env runs fully locally
docker build -t pdf-crawler:local .

docker compose run --rm crawler --site toronto_bylaws --max-documents 5
docker compose run --rm crawler --site ontario_legislative_bills --dry-run
docker compose run --rm crawler --list-sites
```

With no GCP values in `.env`, PDFs go to `data/` and metadata to
`data/_metadata/`. Set `PDF_BUCKET` and `FIRESTORE_PROJECT` and the same image
writes to Cloud Storage and Firestore instead — no code change.

| Flag | Purpose |
|---|---|
| `--site` | which crawler to run (or `CRAWL_SITE`) |
| `--max-documents N` | stop after N new downloads |
| `--min-delay S` | seconds between requests to a host |
| `--dry-run` | discover and log candidates, download nothing |
| `--config` | override config path or `gs://` URI |
| `--show-checkpoint` | print the saved resume position and exit |
| `--no-resume` | ignore the checkpoint and crawl the range from the top |

## Layout

```
common/    config, HTTP+robots, storage, metadata, base crawler
utils/     logging, hashing, URL helpers
ontario_legislative_bills/   crawler.py + config.yaml
toronto_bylaws/              crawler.py + config.yaml
main.py    entrypoint
```

Adding a site: create `<site>/crawler.py` subclassing `BaseCrawler`, implement
`discover()` yielding `PdfCandidate`s newest-first, export `CRAWLER`, and add a
`config.yaml`. Everything else — retries, idempotency, storage, metadata, job
accounting — is inherited.

## Sources

**Ontario** (`ola.org`) — server-rendered. Session index → session page (all
bills, no pagination) → bill page → one PDF per legislative stage
(`b137_e.pdf` first reading, `b002ra_e.pdf` royal assent). Each stage is stored
as its own document so a pipeline can tell a draft from the enacted text.
Traversal: highest parliament → session → bill number.

**Toronto** (`toronto.ca/legdocs/bylaws/`) — PDFs sit at the predictable path
`/legdocs/bylaws/{year}/law{NNNN}.pdf`, so discovery is a HEAD probe over
(year, number). Years descend and numbers descend within a year, so the newest
bylaws come first.

> **Why not the Bylaw Status Registry UI?** The registry page linked in the
> brief is a JavaScript app whose data APIs (`secure.toronto.ca/BlsrPublicRest`,
> `/c3api_data/...`) are behind Akamai bot protection and return *Access Denied*
> to any non-browser client, regardless of headers. Defeating that would mean
> browser-fingerprint evasion, so this crawler uses the canonical PDF store on
> the main host instead: same documents, permitted by `robots.txt`, cheap to
> probe. The trade-off is that registry-only fields (official title, date of
> passing, subject) aren't available — records carry year, bylaw number,
> citation (`712-2026`), `Last-Modified`, size, and hash. If those fields are
> needed later, the options are Toronto's Open Data portal or a
> Playwright-rendered pass against the registry.

## Politeness

- `robots.txt` is fetched per host, cached, and enforced on **every** URL;
  `Crawl-delay` is honoured when longer than the configured floor.
- Minimum delay between requests to a host (1.0–1.5s here) plus jitter, with
  per-host concurrency capped at 2.
- Retries use exponential backoff and honour `Retry-After`; 429/503 always back
  off. Non-retryable statuses (404, 403) fail fast.
- Discovery uses HEAD, which transfers no body.

Lower `min_delay_seconds` at your own risk — these are public-sector servers,
and the crawl is not latency-sensitive.

## Idempotency and failure handling

Each PDF is one Firestore document keyed by a hash of its URL. A re-run skips
anything already `DOWNLOADED` **and** still present in storage — if the object
was deleted, it re-fetches rather than trusting stale metadata. Downloads are
written atomically, so an interrupted run never leaves a truncated PDF that a
later run mistakes for complete.

Document states: `DISCOVERED` → `DOWNLOADED` / `FAILED` / `SKIPPED`. Failures
record the error, HTTP status, and attempt count. A body that isn't really a
PDF (an HTML error page served as 200) is a failure, not a stored file.

Job states: `RUNNING` → `COMPLETED` / `STOPPED_EARLY` / `FAILED` / `STALE`.
`STOPPED_EARLY` means a configured limit was hit (miss streak or document cap)
and is a normal outcome; only `FAILED` exits non-zero so Cloud Run retries it.

### Abandoned jobs (`STALE`)

A job killed without a usable grace period — a console cancellation that
outruns `SIGTERM`, a `SIGKILL`, an OOM, a preempted node — never gets to write
a terminal status and would otherwise sit at `RUNNING` forever.

Running jobs refresh `last_heartbeat_at` every 30s
(`HEARTBEAT_INTERVAL_SECONDS`), on a timer rather than per document so the
write rate stays flat regardless of crawl speed. At startup each crawl sweeps
its own site: any `RUNNING` job whose heartbeat is older than 15 minutes
(`STALE_JOB_AFTER_SECONDS`) is marked `STALE` with the age recorded in
`stop_reason`. Jobs that died before their first heartbeat fall back to
`started_at`. Only task 0 sweeps, and a job never sweeps itself.

`STALE` is deliberately distinct from `FAILED` (the run reported no error — we
simply stopped hearing from it) and from a hypothetical `CANCELLED`, since
Firestore alone can't tell a human cancellation from a preemption. Attributing
that properly would mean reconciling against the Cloud Run Admin API, which
this intentionally avoids.

The threshold is 30× the heartbeat interval, so a slow task is never mistaken
for a dead one. If a live job were swept anyway (say, a Firestore outage
blocked its heartbeats), the effect is transient: when it finishes it writes
its own terminal status over the `STALE` one.

```bash
# Sweep without crawling
docker compose run --rm crawler --site toronto_bylaws --sweep-stale
```

**Stop rule:** 1000 consecutive probes yielding no PDF ends the run completely
(`limits.consecutive_miss_limit`). Within one Toronto year,
`per_unit_miss_limit` consecutive gaps means that year is exhausted and the
crawl moves to the previous one.

## Resuming after a failure

A crawl that dies — task timeout, network failure, preemption, a crash — picks
up where it left off on the next execution rather than restarting.

Each site keeps a checkpoint (Firestore `checkpoints/{site}`, or
`data/_metadata/checkpoints/` locally) holding two positions:

- **frontier** — newest document ever downloaded. Anything above it is new
  material published since the last run.
- **cursor** — oldest position the descending sweep reached. A dead run resumes
  just below this.

Every run does the same two things, in this order:

1. **Resume** the unfinished sweep from below the cursor. Skipped entirely once
   `sweep_complete` is set.
2. **Check for new documents** above the frontier — only the current year
   (Toronto) or newest session (Ontario) can gain them, so this is cheap.

**Firestore is the source of truth, and is consulted on every start** — whether
or not a checkpoint exists. Two ordered, indexed queries resolve the position in
milliseconds:

- `latest_downloaded` — the most recently *written* document. In a descending
  sweep that is the furthest the crawl got, so it becomes the **cursor**.
- `highest_downloaded` — the highest `sort_key`, which becomes the **frontier**.

Each result is then **cross-checked against Cloud Storage**, scoped to that
document's own prefix. A metadata row whose object is missing is skipped and the
next candidate tried, so one stale row can't misdirect the crawl. The resolved
position is written back, so a checkpoint is recreated retroactively even if it
was lost, and a run that dies before its first download still leaves one behind.

The stored checkpoint is still read, but only to supply `sweep_complete` and to
advance the cursor past stretches the sweep probed without finding anything
(lower is further along in a descending sweep).

This replaces an earlier fallback that listed the whole bucket to find a
position — with tens of thousands of objects that was slow enough to look like a
hang. Listing is now always prefix-scoped and never on the startup path.

These queries need composite indexes, applied by the migration runner (below).
Without them Firestore returns `FailedPrecondition`; the crawler logs that
explicitly and degrades rather than failing.

## Migrations

Schema and data changes live in `migrations/` and run as their own Cloud Run
Job (`crawler-migrate`) **before** the crawler jobs are updated — `deploy.sh`
gates the rollout on the migration exit code.

```bash
docker compose run --rm --entrypoint python crawler -m migrations.runner --status
docker compose run --rm --entrypoint python crawler -m migrations.runner --apply --dry-run
docker compose run --rm --entrypoint python crawler -m migrations.runner --apply
```

| Migration | Kind | What it does |
|---|---|---|
| `0001_composite_indexes` | declarative | Creates the resume indexes from `firestore.indexes.json` via the Firestore Admin API |
| `0002_backfill_sort_key` | data | Populates `sort_key` on documents written before the field existed |

Idempotency is layered, because any single mechanism can be defeated:

1. A **ledger** (`_migrations` collection) records what ran, keyed by migration id.
2. A **transactional claim** stops two concurrent deploys applying the same
   migration; a claim older than 30 minutes is treated as abandoned and
   retried.
3. **Migrations are individually re-runnable** — index creation tolerates
   `ALREADY_EXISTS`, the backfill skips documents that already have the field.
   The ledger is then an optimisation and audit trail, not the only safeguard.

A migration edited *after* being applied is reported as `DRIFTED` and neither
re-run nor silently skipped — checksums are compared on every run. Data
migrations checkpoint progress by document id, so a run killed partway resumes
instead of restarting.

**Index builds do not block the deploy.** They can take from minutes to over an
hour, and the crawler already degrades gracefully while they build, so the
runner warns at its timeout (`MIGRATION_INDEX_TIMEOUT`, default 600s) and
continues. Check state with `gcloud firestore indexes composite list`.

Migrations run as a **separate service account** (`pdf-crawler-migrate`) with
`roles/datastore.owner`, since index administration needs rights the crawler
itself should not have.

```bash
docker compose run --rm crawler --site toronto_bylaws --show-checkpoint
docker compose run --rm crawler --site toronto_bylaws --no-resume   # force full re-crawl
```

**Graceful shutdown.** Cloud Run sends `SIGTERM` before killing a task that hits
its timeout or gets preempted. The crawler catches it, finishes the document in
flight, flushes metadata, writes a checkpoint, and exits 0 — so the next
execution continues instead of starting over. Documents are always flushed
*before* the checkpoint, since a checkpoint ahead of its documents would skip
them on resume.

Because of this, a long backfill can be run as repeated bounded executions
rather than one job that must survive for hours.

## Deploy

```bash
./deploy.sh YOUR_PROJECT_ID [REGION] [TASKS]   # default: northamerica-northeast2, 1 task
gcloud run jobs execute crawler-toronto-bylaws --region=REGION
```

**Images are tagged immutably**, by commit SHA (plus a timestamp when the tree
is dirty), never `:latest` alone. A fixed tag makes the `--image` string
identical on every deploy, so `gcloud run jobs update` sees no spec change,
skips the update, and the job keeps executing the digest it first resolved — a
stale rollout that still reports success. The deploy ends by asserting each job
is running the image just built, and fails loudly if not.

Creates Artifact Registry, two buckets, Firestore, a least-privilege service
account, and one Cloud Run Job per site. Triggering is manual by design.

## Parallelism

```bash
./deploy.sh PROJECT_ID REGION 4     # 4 tasks per job
```

Tasks shard on **year** (Toronto) and **session** (Ontario) via
`CLOUD_RUN_TASK_INDEX`, covering disjoint work with no coordination. Ontario
shards at session rather than bill level because the session page must be
fetched to learn its bills at all — finer sharding would make every task
re-fetch every listing page.

Sharding uses a blake2b digest, not the builtin `hash()`, which Python salts per
process: with `hash()` each task computed a *different* map, so work was both
duplicated and silently dropped. Each shard also keeps its own checkpoint
(`{site}--t{i}of{n}`) so tasks never overwrite each other's position.

**Parallelism is a politeness decision, not just a throughput one.** Each task
runs its own rate limiter, so N tasks means N× the request rate at a single
government host. Measured: network time is only ~10-20% of the pacing interval,
so in-process concurrency would buy almost nothing — the speedup comes entirely
from the added request rate. 2-4 tasks for a backfill, 1 for routine runs.

Cost is dominated by storage retention, not compute — idle cost is the bucket
alone. `lifecycle.json` transitions PDFs to Nearline at 30 days and Coldline at
90 to cut that further.
