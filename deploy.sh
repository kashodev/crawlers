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
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/crawler:latest"
PDF_BUCKET="${PROJECT}-pdf-archive"
CONFIG_BUCKET="${PROJECT}-crawler-config"
SA="pdf-crawler"
SA_EMAIL="${SA}@${PROJECT}.iam.gserviceaccount.com"
# Migrations create indexes, which the crawler's own account must not be able
# to do — so they run as a separate identity with a broader Firestore role.
MIGRATE_SA="pdf-crawler-migrate"
MIGRATE_SA_EMAIL="${MIGRATE_SA}@${PROJECT}.iam.gserviceaccount.com"
MIGRATE_JOB="crawler-migrate"

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

# datastore.owner covers index administration, which datastore.user does not.
bind_with_retry "datastore.owner on ${PROJECT}" \
  gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${MIGRATE_SA_EMAIL}" --role=roles/datastore.owner

echo "==> Uploading site configs"
for site in ontario_legislative_bills toronto_bylaws; do
  gcloud storage cp "${site}/config.yaml" "gs://${CONFIG_BUCKET}/sites/${site}.yaml"
done

echo "==> Building and pushing image (linux/amd64 for Cloud Run)"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet >/dev/null
docker build --platform linux/amd64 -t "${IMAGE}" .
docker push "${IMAGE}"

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
for site in ontario_legislative_bills toronto_bylaws; do
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

cat <<EOF

Done. Trigger a crawl manually:

  gcloud run jobs execute crawler-toronto-bylaws --region=${REGION}
  gcloud run jobs execute crawler-ontario-legislative-bills --region=${REGION}

Follow logs:

  gcloud beta run jobs logs tail crawler-toronto-bylaws --region=${REGION}

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
