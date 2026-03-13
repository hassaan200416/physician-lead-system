# Physician Lead System Backend

Production backend for ingesting, validating, scoring, and serving physician leads.

## What This Service Does

- Ingests CMS NPPES physician data.
- Validates and normalizes records.
- Computes lead scores and tiers.
- Enriches contact data (email pipeline).
- Serves REST APIs for physicians and leads.
- Supports scheduled weekly sync.

## Tech Stack

- Python 3.11
- FastAPI
- SQLAlchemy 2.x
- Alembic
- PostgreSQL (Supabase-hosted or self-hosted)
- pytest

## Repository Structure

- api: FastAPI app and routes
- etl: ingestion, validation, scoring, enrichment
- models: SQLAlchemy models
- migrations: Alembic migration scripts
- reference_data: static mapping/reference inputs
- raw_data: input CSV storage
- tests: unit and integration-style tests
- database.py: shared engine/session setup
- scheduler.py: weekly ETL scheduler

## Prerequisites

- Python 3.11+
- PostgreSQL-compatible database URL
- Optional: Hunter API key for email enrichment pipeline

## Environment Variables

Create backend/.env:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<db>
HUNTER_API_KEY=<optional_for_email_enrichment>
```

Notes:

- database.py converts postgresql:// to SQLAlchemy's postgresql+psycopg:// automatically.
- Do not commit secrets.

## Local Setup

From repository root:

```bash
cd backend
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

macOS/Linux:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Apply migrations:

```bash
alembic upgrade head
```

Load reference data (first run):

```bash
python reference_data/load_reference_data.py
```

## Running the API

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

OpenAPI docs:

- http://127.0.0.1:8000/docs

Health checks:

- GET /
- GET /health

## Core API Endpoints

### Physicians

- GET /api/v1/physicians/
- GET /api/v1/physicians/stats
- GET /api/v1/physicians/{npi}

### Leads

- GET /api/v1/leads/export
- GET /api/v1/leads/sync-logs
- POST /api/v1/leads/{npi}/call-outcome
- GET /api/v1/leads/pipeline/summary

## ETL Workflows

Run ingest against a downloaded NPPES file:

```bash
python etl/ingest_nppes.py --file raw_data/nppes/<npidata_file>.csv
```

Run email enrichment for a subset:

```bash
python etl/enrich_emails.py --limit 25
```

Run email enrichment for a single NPI:

```bash
python etl/enrich_emails.py --npi <10_digit_npi>
```

## Scheduler

Run weekly scheduler (Sunday 02:00):

```bash
python scheduler.py
```

Run immediately:

```bash
python scheduler.py --now
```

## Testing

```bash
pytest tests -v
```

## Operational Practices

- Keep migrations forward-only and reviewed.
- Use parameterized SQL or SQLAlchemy APIs only.
- Restrict CORS allow_origins in production.
- Rotate keys and database credentials regularly.
- Add alerts on ETL failures and health endpoint degradation.
- Capture ETL run metrics (processed/failed/error_rate).

## Deployment Notes

- Run API behind a reverse proxy with TLS.
- Use process supervision (systemd, Docker, or managed runtime).
- Pin dependency versions and rebuild from lock/requirements.
- Run alembic upgrade head as part of release workflow.

## Troubleshooting

Database startup failure:

- Verify DATABASE_URL in backend/.env.
- Confirm database network access and credentials.

No NPPES file found for scheduler:

- Ensure CSV exists under backend/raw_data/nppes.

Email enrichment skipped:

- Confirm HUNTER_API_KEY is present for enrich_emails.py.
