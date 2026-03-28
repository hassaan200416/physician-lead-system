# Backend — Physician Lead System

Python backend for ingesting, validating, scoring, enriching, and serving US physician lead data.

---

## Table of Contents

1. [Stack](#stack)
2. [Directory Structure](#directory-structure)
3. [Local Setup](#local-setup)
4. [Environment Variables](#environment-variables)
5. [Database Setup](#database-setup)
6. [Running the API](#running-the-api)
7. [ETL Pipeline](#etl-pipeline)
8. [Enrichment Scripts](#enrichment-scripts)
9. [Scheduler](#scheduler)
10. [Testing](#testing)
11. [Database Schema](#database-schema)
12. [API Reference](#api-reference)
13. [Operational Notes](#operational-notes)
14. [Troubleshooting](#troubleshooting)

---

## Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.11 | Runtime |
| FastAPI | 0.115 | REST API framework |
| Uvicorn | 0.30 | ASGI server |
| SQLAlchemy | 2.0 | ORM + raw SQL |
| Alembic | 1.13 | Database migrations |
| psycopg3 | 3.2 | PostgreSQL driver |
| pandas | 3.0 | CSV processing |
| pytest | 8.3 | Test runner |
| schedule | 1.2 | Weekly sync cron |
| python-dotenv | 1.0 | `.env` loading |

---

## Directory Structure

```
backend/
├── api/
│   ├── main.py                         # FastAPI app, CORS, router registration
│   ├── routes/
│   │   ├── physicians.py               # GET /api/v1/physicians/*
│   │   └── leads.py                    # GET/POST /api/v1/leads/*
│   └── schemas/
│       └── physician_schema.py         # Pydantic request/response models
│
├── etl/
│   ├── ingest_nppes.py                 # Main ETL: CSV → physician/license/location tables
│   ├── validate_records.py             # Pure validation functions (no DB calls)
│   ├── compute_scores.py               # 4-pillar scoring + contact_category logic
│   ├── download_nppes.py               # Finds latest NPPES file in raw_data/
│   ├── enrich_phones.py                # Phone: PeopleDataLabs → Twilio → Telnyx DNC
│   ├── enrich_emails.py                # Email: Hunter.io (2-pass + DNS pre-filters)
│   ├── enrich_contactout.py            # Email: ContactOut API
│   ├── enrich_contactout_linkedin.py   # Email: ContactOut LinkedIn variant
│   ├── enrich_fullenrich.py            # Email: FullEnrich API
│   └── enrich_fullenrich_csv.py        # Email: FullEnrich CSV import
│
├── models/
│   ├── physician.py                    # Central physician record (~80 columns)
│   ├── practice_location.py            # physician_practice_locations table
│   ├── license.py                      # license table
│   ├── organization.py                 # organization_master + link table
│   ├── enrichment_source.py            # enrichment_source_stats table
│   └── metadata.py                     # field_value_history audit trail
│
├── migrations/
│   ├── env.py
│   ├── 340e952d67cc_create_all_base_tables.py
│   ├── 002_add_phone_and_contact_completeness.py
│   └── 003_enrichment_sources_and_stats.py
│
├── reference_data/
│   ├── nucc_taxonomy.csv               # Medical specialty codes
│   ├── excluded_taxonomies.csv         # Non-physician specialties to filter out
│   ├── source_registry.csv             # Enrichment source seed data
│   ├── zip_state_reference.csv         # ZIP-to-state mapping
│   └── load_reference_data.py          # Seeds the above into the DB
│
├── raw_data/
│   └── nppes/                          # Drop the NPPES CSV here
│
├── exports/                            # Output files from enrichment CSV imports
│
├── tests/
│   ├── test_etl.py                     # 54 unit tests
│   └── test_validation.py
│
├── database.py                         # Engine, SessionLocal, Base, get_db()
├── scheduler.py                        # Weekly ETL automation
├── alembic.ini
└── requirements.txt
```

---

## Local Setup

### 1. Create and activate a virtual environment

```bash
cd backend
python -m venv venv
```

```bash
# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Environment Variables

Create `backend/.env`. This file is gitignored — never commit it.

```env
# Required — Supabase PostgreSQL connection string
# Use postgresql:// format; the app rewrites it to postgresql+psycopg:// automatically
DATABASE_URL=postgresql://postgres:<password>@<host>:5432/<db>

# Phone enrichment (enrich_phones.py)
PDL_API_KEY=
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TELNYX_API_KEY=

# Email enrichment (enrich_emails.py)
HUNTER_API_KEY=

# Additional email enrichment
CONTACTOUT_API_KEY=
FULLENRICH_API_KEY=
LUSHA_API_KEY=
```

The API server and ETL ingest script will start without enrichment keys. Only the relevant enrichment script will fail if its key is missing.

---

## Database Setup

### Apply all migrations

```bash
alembic upgrade head
```

This creates all tables in the target database. Run this once on a fresh database and again after each migration is added.

### Load static reference data

```bash
python reference_data/load_reference_data.py
```

Loads four reference tables into the database:
- `nucc_taxonomy_reference` — NUCC physician specialty codes used for taxonomy filtering
- `excluded_taxonomy_codes` — non-physician codes (nurse practitioners, therapists, etc.) to exclude
- `enrichment_source_stats` — one seed row per enrichment tool (Hunter, PDL, ContactOut, etc.)
- ZIP-to-state reference data

**Run this once on a fresh database before running the ETL.**

---

## Running the API

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- API docs (Swagger UI): http://localhost:8000/docs
- Health check: http://localhost:8000/health

The `--reload` flag restarts the server on code changes. Remove it in production.

---

## ETL Pipeline

### Download NPPES data

Download the full NPI data file from the [CMS NPPES download page](https://download.cms.gov/nppes/NPI_Files.html). The filename follows the format `npidata_pfile_YYYYMMDD-YYYYMMDD.csv`. Place it in `backend/raw_data/nppes/`.

The full file is ~8 million rows and about 8 GB. The ETL is chunked (50,000 rows at a time) so it will not run out of memory.

### Run the ETL

From the `backend/` directory:

```bash
# Auto-detect the latest file in raw_data/nppes/ and process it fully
python etl/ingest_nppes.py

# Test with a small batch before committing to a full run
python etl/ingest_nppes.py --limit 5000

# Point at a specific file explicitly
python etl/ingest_nppes.py --file raw_data/nppes/npidata_pfile_20050523-20260308.csv
```

**What it does:**
1. Loads reference data (taxonomy map, excluded codes, ZIP-state map, license format rules) into memory
2. Reads the CSV in 50,000-row chunks using pandas
3. For each row: validates NPI (Luhn algorithm), normalises names and credentials, selects primary taxonomy code, validates address and ZIP, validates licenses, calculates experience from graduation year
4. Computes the 4-pillar lead score (0–100) using `compute_scores.compute_lead_score()`
5. Upserts into `physician`, `physician_practice_locations`, and `license` tables
6. Writes a summary row to `sync_log` on completion

A full run takes approximately 30–60 minutes depending on hardware. Progress is printed to stdout every chunk.

---

## Enrichment Scripts

All enrichment scripts follow the same pattern:
1. Query `physician` for records not yet attempted by that source (`enrichment_sources` array does not contain the source name)
2. Call the external API
3. Write `personal_email` and/or `mobile_phone` to `physician`
4. Append the source name to `physician.enrichment_sources[]`
5. Increment stats in `enrichment_source_stats` atomically
6. Call `sync_to_leads()` to upsert the physician into the `leads` table

### Phone enrichment — PeopleDataLabs + Twilio + Telnyx

```bash
python etl/enrich_phones.py                    # all unattempted physicians
python etl/enrich_phones.py --limit 50         # batch of 50
python etl/enrich_phones.py --npi 1234567890   # single physician (testing)
python etl/enrich_phones.py --preview          # check PDL coverage before purchasing
python etl/enrich_phones.py --backfill         # push all A/B/C physicians to leads table
```

**Two-pass PDL strategy:**
- Pass 1: name + city + state (min likelihood 7)
- Pass 2: add practice phone or email as extra signal (min likelihood 8)

**After PDL returns a number:**
- Twilio Lookup v2 confirms line type (mobile/landline/VOIP) — landlines are skipped
- Telnyx DNC API checks federal + state + wireless DNC registries — numbers on DNC are not stored
- Phone confidence is set to HIGH (likelihood ≥ 8) or MEDIUM (likelihood ≥ 7)
- `phone_state_risk` is set based on TCPA state profiles (CA/FL = CRITICAL, others HIGH/MEDIUM)

### Email enrichment — Hunter.io

```bash
python etl/enrich_emails.py --limit 25         # process up to 25 (free tier limit)
python etl/enrich_emails.py --npi 1234567890   # single physician (testing)
```

**Two-pass Hunter strategy:**
- Pass 1: company name → Hunter resolves domain + returns email + score
- Pass 2: if score < 70, retry with the discovered domain directly (typically +15–25 pts)

**Five free DNS pre-filters run after Hunter returns an email:**
1. Syntax check
2. Disposable domain check
3. DNS A record check (domain exists)
4. MX record check (domain can receive email)
5. Catch-all SMTP probe (rejects domains that accept all addresses)

**Confidence levels:**
- `HIGH` — Hunter-verified or score ≥ 70 and not catch-all
- `MEDIUM` — score 40–69 and not catch-all
- `LOW` — catch-all domain

### ContactOut enrichment

```bash
python etl/enrich_contactout.py
python etl/enrich_contactout_linkedin.py
```

### FullEnrich enrichment

```bash
python etl/enrich_fullenrich.py
python etl/enrich_fullenrich_csv.py   # import from a pre-exported CSV
```

---

## Scheduler

The scheduler runs `ingest_nppes.py` automatically every Sunday at 02:00 AM. It finds the most recent NPPES file in `raw_data/nppes/` by sorting filenames lexicographically (which equals chronological order given the CMS naming format).

```bash
# Start the scheduler (keep running in the background)
python scheduler.py

# Run the ETL immediately without waiting for the schedule
python scheduler.py --now
```

For production, run the scheduler as a background service using `systemd`, `supervisor`, or a Docker container with a restart policy.

---

## Testing

```bash
pytest tests/ -v
```

**54 tests covering:**
- NPI validation (Luhn algorithm with 80840 prefix)
- Name normalisation (O'Connor, Smith-Jones, edge cases)
- Address validation and confidence scoring
- License number format validation
- Graduation year validation and experience bucket calculation
- Gender normalisation
- Lead scoring (all four pillars)

Run a specific test file:

```bash
pytest tests/test_etl.py -v
pytest tests/test_validation.py -v
```

---

## Database Schema

### Core tables

**`physician`** — master record, one row per NPI (~80 columns)

| Column group | Key columns |
|-------------|------------|
| Identity | `npi` (PK), `entity_type`, `is_active` |
| Names | `first_name_raw`, `last_name_raw`, `first_name_clean`, `last_name_clean`, `full_name_display`, `credential_normalized` |
| Specialty | `primary_taxonomy_code`, `specialty_name`, `derived_specialty_category` |
| Experience | `graduation_year`, `years_of_experience`, `experience_bucket` |
| Phone | `mobile_phone`, `phone_confidence`, `phone_line_type`, `phone_dnc_clear`, `phone_state_risk` |
| Email (canonical) | `personal_email`, `personal_email_confidence`, `practice_email` |
| Email (legacy) | `email`, `email_confidence_level` — **read-only Hunter v1 data** |
| Enrichment audit | `enrichment_sources[]`, `enrichment_last_attempted_at` |
| Scoring | `lead_score_current`, `lead_tier` |
| Calling feedback | `last_call_outcome`, `call_attempt_count`, `last_called_at` |

**`physician_practice_locations`** — practice addresses (one physician → many locations)

**`license`** — medical licenses (one physician → many states)

**`organization_master`** — practice entities discovered during ingest

**`leads`** — denormalised flat view for the frontend; only physicians with ≥ 1 contact signal; written by enrichment scripts via `sync_to_leads()`

**`enrichment_source_stats`** — cumulative stats per enrichment tool (emails_provided, phones_provided, hit_rate, last_used_at)

**`sync_log`** — one row per ETL run (records_processed, records_inserted, records_updated, error_rate_pct, status)

### Migrations

```bash
# Apply all pending migrations
alembic upgrade head

# See current migration state
alembic current

# See migration history
alembic history
```

Migration files live in `migrations/versions/`. Always create a new migration for schema changes — never modify existing migration files.

---

## API Reference

### Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | System status check |
| GET | `/health` | Database connectivity check |

### Physicians

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/physicians/` | Paginated list with filters (state, specialty_category, tier, experience_bucket, min_score, is_active) |
| GET | `/api/v1/physicians/stats` | Aggregate counts by tier, specialty, state |
| GET | `/api/v1/physicians/{npi}` | Full profile for one physician |

### Leads

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/leads/` | Paginated lead list (tier, contact_completeness, state filters) |
| GET | `/api/v1/leads/export` | Download filtered leads as CSV (max 5,000 rows) |
| GET | `/api/v1/leads/sync-logs` | Recent ETL run history |
| POST | `/api/v1/leads/{npi}/call-outcome` | Record call result (answered/voicemail/do_not_call/etc.) |
| GET | `/api/v1/leads/pipeline/summary` | Tier breakdown, experience breakdown, top states |

Full interactive documentation with request/response schemas is available at **http://localhost:8000/docs** when the API is running.

---

## Operational Notes

- **CORS**: `allow_origins=["*"]` is set in `api/main.py`. Restrict this to your frontend domain before deploying to production.
- **Connection pooling**: `pool_size=5`, `max_overflow=10`. Tune these based on your Supabase plan's connection limit.
- **SQL injection**: All queries use parameterised SQLAlchemy `text()` calls. Never use string formatting to build SQL.
- **Migrations**: Always forward-only. Never modify an already-applied migration file.
- **Enrichment keys**: Missing API keys cause the enrichment script to exit cleanly with a warning — they do not crash the API.
- **TCPA compliance**: Phone enrichment sets `phone_state_risk` on every record. CA and FL are CRITICAL — do not dial mobile numbers in those states without explicit consent.

---

## Troubleshooting

**`DATABASE_URL is not set in .env file` on startup**
→ Create `backend/.env` and add the `DATABASE_URL` line.

**`Database connection failed` on startup**
→ Check that your Supabase project is active and the connection string is correct. Verify network access if connecting from behind a firewall.

**`alembic upgrade head` fails with "relation already exists"**
→ The database already has some tables. Check `alembic current` to see the current revision. If starting fresh, drop all tables and re-run `alembic upgrade head`.

**ETL exits immediately with "No NPPES file found"**
→ No CSV matching `npidata_pfile*.csv` was found in `raw_data/nppes/`. Download the file from CMS and place it there.

**Hunter enrichment skips all physicians**
→ Either `HUNTER_API_KEY` is missing in `.env`, or all eligible physicians have already been attempted. Check `physician.enrichment_sources` — if `hunter.io` is already in the array, those records are skipped by design.

**Phone enrichment returns "PDL_API_KEY not configured"**
→ Add `PDL_API_KEY` to `backend/.env`.
