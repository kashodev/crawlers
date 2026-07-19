# Slim runtime image for Cloud Run Jobs. Keeping the image small matters twice:
# Artifact Registry bills per GB-month, and cold-start pull time is billed too.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first so code edits don't invalidate the wheel layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY utils/ ./utils/
COPY migrations/ ./migrations/
COPY ontario_legislative_bills/ ./ontario_legislative_bills/
COPY toronto_bylaws/ ./toronto_bylaws/
COPY firestore.indexes.json main.py ./

# Non-root: Cloud Run doesn't require it, but there's no reason to run as root.
RUN useradd --create-home --uid 1000 crawler \
    && mkdir -p /app/data \
    && chown -R crawler:crawler /app
USER crawler

# Runs once and exits — the run-to-completion contract for Cloud Run Jobs.
# Site comes from CRAWL_SITE, or from args passed to `gcloud run jobs execute`.
ENTRYPOINT ["python", "main.py"]
