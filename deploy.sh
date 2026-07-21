#!/usr/bin/env bash
# Deploy the crawler to Cloud Run Jobs, following the architecture in the
# deployment guide: Cloud Run Jobs + Cloud Storage + Firestore + Artifact
# Registry, with manual (gcloud) triggering.
#
# Idempotent: re-running updates the job rather than failing on existing
# resources. Requires gcloud, docker, and an authenticated account.
#
#   ./deploy.sh PROJECT_ID [REGION] [TASKS]

set -euo pipefail

PROJECT="${1:?usage: ./deploy.sh PROJECT_ID [REGION] [TASKS]}"
REGION="${2:-northamerica-northeast2}"   # Toronto region: closest to the sources
# Parallel tasks per job. Each task runs its own rate limiter, so N tasks means
# N times the request rate at the origin — this is a politeness decision, not
# just a throughput one. 1 for routine runs; 2-4 is reasonable for a backfill.
TASKS="${3:-1}"
REPO="crawler-repo"
# Immutable, unique tag per deploy.
#
# A fixed tag like :latest makes the --image string identical on every deploy,
# so `gcloud run jobs update` sees no spec change, skips the update, and the job
# goes on executing whatever digest it resolved the first time — a silent stale
# rollout. A tag that changes whenever the build does forces the update through.
#
# Clean tree -> the commit SHA, so a deployed image is traceable to source.
# Dirty tree -> SHA plus a timestamp, because the SHA alone would describe code
# that is not what is in the image.
IMAGE_REPO="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/crawler"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
if git rev-parse --git-dir >/dev/null 2>&1 && ! git diff-index --quiet HEAD -- 2>/dev/null; then
  IMAGE_TAG="${GIT_SHA}-dirty-$(date -u +%Y%m%d%H%M%S)"
  echo "NOTE: uncommitted changes present; tagging image ${IMAGE_TAG}"
else
  IMAGE_TAG="${GIT_SHA}"
fi
IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"
IMAGE_LATEST="${IMAGE_REPO}:latest"
PDF_BUCKET="${PROJECT}-pdf-archive"
CONFIG_BUCKET="${PROJECT}-crawler-config"
SA="pdf-crawler"
SA_EMAIL="${SA}@${PROJECT}.iam.gserviceaccount.com"
# Migrations create indexes, which the crawler's own account must not be able
# to do — so they run as a separate identity with a broader Firestore role.
MIGRATE_SA="pdf-crawler-migrate"
MIGRATE_SA_EMAIL="${MIGRATE_SA}@${PROJECT}.iam.gserviceaccount.com"
MIGRATE_JOB="crawler-migrate"

# The one place site modules are listed. Config upload, job creation, and the
# closing "how to trigger" hints all derive from this, so adding a site here
# updates every step — no more drift between the jobs and the printed commands.
SITES=(ontario_legislative_bills toronto_bylaws ontario_elaws canada_federal_laws)

echo "==> Project ${PROJECT} / region ${REGION}"
gcloud config set project "${PROJECT}" >/dev/null

echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  firestore.googleapis.com \
  storage.googleapis.com \
  cloudscheduler.googleapis.com >/dev/null

echo "==> Artifact Registry"
gcloud artifacts repositories describe "${REPO}" --location="${REGION}" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "${REPO}" \
    --repository-format=docker --location="${REGION}" \
    --description="PDF crawler images"

echo "==> Buckets (single-region, Standard: no multi-region premium)"
for bucket in "${PDF_BUCKET}" "${CONFIG_BUCKET}"; do
  gcloud storage buckets describe "gs://${bucket}" >/dev/null 2>&1 || \
    gcloud storage buckets create "gs://${bucket}" \
      --location="${REGION}" --default-storage-class=STANDARD \
      --uniform-bucket-level-access
done

echo "==> Firestore (Native mode)"
gcloud firestore databases describe --database="(default)" >/dev/null 2>&1 || \
  gcloud firestore databases create --location="${REGION}" --type=firestore-native

echo "==> Service account (least privilege: these two resources only)"
gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "${SA}" --display-name="PDF crawler job"

# A freshly created service account is not immediately visible to the IAM
# policy APIs, which then reject bindings with "does not exist". Wait for it to
# propagate rather than racing it.
echo "==> Waiting for service account to propagate"
for attempt in $(seq 1 30); do
  if gcloud iam service-accounts describe "${SA_EMAIL}" >/dev/null 2>&1; then
    break
  fi
  if [[ ${attempt} -eq 30 ]]; then
    echo "ERROR: ${SA_EMAIL} still not visible after 60s" >&2
    exit 1
  fi
  sleep 2
done

# Even after it is describable, policy propagation can lag a few more seconds,
# so retry the bindings themselves instead of failing the whole deploy.
bind_with_retry() {
  local description="$1"; shift
  for attempt in $(seq 1 10); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    if [[ ${attempt} -eq 10 ]]; then
      echo "ERROR: ${description} failed after 10 attempts; last error:" >&2
      "$@" >&2 || true   # re-run once unsuppressed so the real error is visible
      return 1
    fi
    sleep 3
  done
}

for bucket in "${PDF_BUCKET}" "${CONFIG_BUCKET}"; do
  bind_with_retry "objectAdmin on gs://${bucket}" \
    gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
      --member="serviceAccount:${SA_EMAIL}" --role=roles/storage.objectAdmin
done
bind_with_retry "datastore.user on ${PROJECT}" \
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA_EMAIL}" --role=roles/datastore.user

echo "==> Migration service account"
gcloud iam service-accounts describe "${MIGRATE_SA_EMAIL}" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "${MIGRATE_SA}" --display-name="PDF crawler migrations"

for attempt in $(seq 1 30); do
  gcloud iam service-accounts describe "${MIGRATE_SA_EMAIL}" >/dev/null 2>&1 && break
  [[ ${attempt} -eq 30 ]] && { echo "ERROR: ${MIGRATE_SA_EMAIL} not visible" >&2; exit 1; }
  sleep 2
done

# datastore.owner covers data access for backfills. indexAdmin is granted
# explicitly because it is the role documented for create/list/delete on
# composite indexes — do not rely on owner implying it.
bind_with_retry "datastore.owner on ${PROJECT}" \
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${MIGRATE_SA_EMAIL}" --role=roles/datastore.owner
bind_with_retry "datastore.indexAdmin on ${PROJECT}" \
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${MIGRATE_SA_EMAIL}" --role=roles/datastore.indexAdmin

# IAM changes are eventually consistent and can take minutes to take effect.
# The image build below usually covers it, but a short explicit pause makes the
# common case reliable rather than lucky.
echo "    waiting 30s for IAM to propagate"
sleep 30

echo "==> Uploading site configs"
for site in "${SITES[@]}"; do
  gcloud storage cp "${site}/config.yaml" "gs://${CONFIG_BUCKET}/sites/${site}.yaml"
done

echo "==> Building and pushing image (linux/amd64 for Cloud Run)"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet >/dev/null
# :latest is pushed alongside as a convenience pointer, but the job specs
# always reference the immutable tag.
docker build --platform linux/amd64 -t "${IMAGE}" -t "${IMAGE_LATEST}" .
docker push "${IMAGE}"
docker push "${IMAGE_LATEST}"
echo "    deploying image: ${IMAGE}"

# Cloud Run resolves a tag to a digest and may report the digest form rather
# than the tag, so verification needs to accept either. Empty is fine; the
# check below then falls back to comparing tags only.
IMAGE_DIGEST="$(docker inspect --format='{{range .RepoDigests}}{{println .}}{{end}}' "${IMAGE}" 2>/dev/null \
  | grep -F "${IMAGE_REPO}" | head -1 | sed 's/.*@//')"
[[ -n "${IMAGE_DIGEST}" ]] && echo "    digest: ${IMAGE_DIGEST}"

echo "==> Migration job"
# Same image, different entrypoint. Runs in-cluster as the migration service
# account, so nobody needs Firestore admin rights locally to deploy.
migrate_args=(
  --image="${IMAGE}"
  --region="${REGION}"
  --service-account="${MIGRATE_SA_EMAIL}"
  --cpu=1 --memory=512Mi
  --max-retries=1
  --task-timeout=1800
  --tasks=1
  --command="python"
  --args="-m,migrations.runner,--apply"
  --set-env-vars="FIRESTORE_PROJECT=${PROJECT},MIGRATION_INDEX_TIMEOUT=600"
)

if gcloud run jobs describe "${MIGRATE_JOB}" --region="${REGION}" >/dev/null 2>&1; then
  gcloud run jobs update "${MIGRATE_JOB}" "${migrate_args[@]}"
else
  gcloud run jobs create "${MIGRATE_JOB}" "${migrate_args[@]}"
fi

echo "==> Running migrations (indexes + data backfills) before the crawlers"
# Gate the crawler rollout on this: a failed data migration means the new code
# would run against a schema it does not expect.
if ! gcloud run jobs execute "${MIGRATE_JOB}" --region="${REGION}" --wait; then
  echo "" >&2
  echo "ERROR: migrations failed — crawler jobs were NOT updated." >&2
  echo "Inspect with:" >&2
  echo "  gcloud beta run jobs logs tail ${MIGRATE_JOB} --region=${REGION}" >&2
  exit 1
fi

# One job per site: they have different runtimes and politeness budgets, and
# separate jobs keep their logs, retries, and failures independently visible.
for site in "${SITES[@]}"; do
  job="crawler-${site//_/-}"
  echo "==> Cloud Run Job ${job}"

  # 1 vCPU / 1 GiB: this workload downloads and stores, it doesn't parse PDFs.
  # A task that hits --task-timeout gets SIGTERM, which the crawler handles by
  # checkpointing — so a retry resumes rather than restarting. Same for the two
  # automatic retries.
  args=(
    --image="${IMAGE}"
    --region="${REGION}"
    --service-account="${SA_EMAIL}"
    --cpu=1 --memory=1Gi
    --max-retries=2
    --task-timeout=3600
    --tasks="${TASKS}"
    --set-env-vars="CRAWL_SITE=${site},PDF_BUCKET=${PDF_BUCKET},PDF_PREFIX=${site},FIRESTORE_PROJECT=${PROJECT},CONFIG_URI=gs://${CONFIG_BUCKET}/sites/${site}.yaml"
  )

  if gcloud run jobs describe "${job}" --region="${REGION}" >/dev/null 2>&1; then
    gcloud run jobs update "${job}" "${args[@]}"
  else
    gcloud run jobs create "${job}" "${args[@]}"
  fi
done

echo "==> Verifying deployed image"
# Guards against a silent stale rollout: a skipped update leaves the job on an
# older image while the deploy still reports success.
#
# The image is read out of the raw JSON rather than a typed field path, because
# the nesting differs between the Knative-style and v2 job representations and
# a wrong path yields an empty string that looks exactly like a failure.
deployed_image_of() {
  gcloud run jobs describe "$1" --region="${REGION}" --format=json 2>/dev/null \
    | grep -o '"image"[[:space:]]*:[[:space:]]*"[^"]*"' \
    | head -1 \
    | sed 's/.*"image"[[:space:]]*:[[:space:]]*"\(.*\)"/\1/'
}

verify_failed=0
unverified=0
for job in "${MIGRATE_JOB}" crawler-ontario-legislative-bills crawler-toronto-bylaws; do
  deployed="$(deployed_image_of "${job}")"

  if [[ -z "${deployed}" ]]; then
    # Could not read the field. That is a gap in this check, not evidence of a
    # bad deploy, so it must not fail the run.
    echo "    ?   ${job}: could not read deployed image; skipping verification" >&2
    unverified=1
  elif [[ "${deployed}" == "${IMAGE}" ]]; then
    echo "    OK  ${job} -> ${IMAGE_TAG}"
  elif [[ -n "${IMAGE_DIGEST}" && "${deployed}" == *"@${IMAGE_DIGEST}" ]]; then
    # Same image, reported in digest form.
    echo "    OK  ${job} -> ${IMAGE_TAG} (by digest)"
  else
    echo "    STALE  ${job}" >&2
    echo "           running:  ${deployed}" >&2
    echo "           expected: ${IMAGE}" >&2
    verify_failed=1
  fi
done

if [[ ${unverified} -eq 1 ]]; then
  echo "    NOTE: verify manually with:" >&2
  echo "      gcloud run jobs describe ${MIGRATE_JOB} --region=${REGION} --format=json | grep image" >&2
fi

if [[ ${verify_failed} -eq 1 ]]; then
  echo "" >&2
  echo "ERROR: at least one job is not running the image just built." >&2
  echo "Re-run the deploy; if it persists, set the image explicitly:" >&2
  echo "  gcloud run jobs update JOB --region=${REGION} --image=${IMAGE}" >&2
  exit 1
fi

# Build the per-site trigger hints from the same list the jobs were created
# from, so this can never list a different set than what was deployed.
TRIGGER_HINTS=""
for site in "${SITES[@]}"; do
  TRIGGER_HINTS+="  gcloud run jobs execute crawler-${site//_/-} --region=${REGION}"$'\n'
done
FIRST_JOB="crawler-${SITES[0]//_/-}"

cat <<EOF

Deployed image: ${IMAGE}

Done. Trigger a crawl manually:

${TRIGGER_HINTS}
Follow logs (any job):

  gcloud beta run jobs logs tail ${FIRST_JOB} --region=${REGION}

PDFs:     gs://${PDF_BUCKET}/
Metadata: Firestore collections 'documents', 'jobs', and 'checkpoints'

Crawls resume automatically: a run that times out or fails checkpoints its
position, and the next execution continues from there, then checks for new
documents. Re-executing the same job is the way to continue a large backfill.
To force a full re-crawl instead:

  gcloud run jobs execute JOB --region=${REGION} --update-env-vars=CRAWLER_NO_RESUME=1

Cost note: idle cost is storage only — Cloud Run Jobs bills nothing between runs.
To cut retention cost, add a lifecycle rule moving PDFs to Nearline after 30 days:
  gcloud storage buckets update gs://${PDF_BUCKET} --lifecycle-file=lifecycle.json
EOF
