# Physician Lead System

A full-stack lead generation platform built around the US federal NPPES physician registry. It ingests, validates, scores, and enriches physician records, then serves them through a React dashboard for outreach teams.

---

## Table of Contents

1. [What the System Does](#what-the-system-does)
2. [Architecture Overview](#architecture-overview)
3. [Repository Structure](#repository-structure)
4. [Prerequisites](#prerequisites)
5. [Quick Start](#quick-start)
6. [Environment Variables](#environment-variables)
7. [How the Data Pipeline Works](#how-the-data-pipeline-works)
8. [Lead Scoring Model](#lead-scoring-model)
9. [Contact Categories](#contact-categories)
10. [Running the Services](#running-the-services)
11. [Key Concepts to Understand First](#key-concepts-to-understand-first)

---

## What the System Does

1. **Ingests** the free CMS NPPES bulk CSV (~8 million rows) into a PostgreSQL database, validating and normalising every field.
2. **Scores** each physician 0–100 across four pillars (contact reachability, practice structure, NPI activity, and specialty fit).
3. **Enriches** contact data by calling external APIs — PeopleDataLabs + Twilio for mobile phones, Hunter.io / ContactOut / FullEnrich for emails.
4. **Populates a `leads` table** — a denormalised, query-optimised flat view containing only physicians who have at least one contact signal (email or phone). This is what the frontend reads.
5. **Serves** a FastAPI REST API and a React + Supabase frontend that lets an outreach team browse, filter, search, and review leads.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    DATA PIPELINE                        │
│                                                         │
│  CMS NPPES CSV (weekly)                                 │
│       │                                                 │
│       ▼                                                 │
│  ingest_nppes.py  ──► physician table                   │
│  (validate + score)   license table                     │
│                        physician_practice_locations      │
│                                                         │
│  enrich_phones.py ──► PDL → Twilio → Telnyx DNC        │
│  enrich_emails.py ──► Hunter.io (2-pass + DNS filters)  │
│  enrich_contactout.py, enrich_fullenrich.py, …          │
│       │                                                 │
│       ▼                                                 │
│  leads table  (only physicians with contact data)       │
└─────────────────────────────────────────────────────────┘
          │                          │
          ▼                          ▼
   FastAPI REST API          Supabase (direct)
   /api/v1/physicians        leads table
   /api/v1/leads             lead_reviews table
          │                          │
          └──────────┬───────────────┘
                     ▼
              React Frontend
              Dashboard / Leads / Activity
```

**Both the FastAPI backend and the React frontend connect to the same Supabase-hosted PostgreSQL database.** The frontend reads the `leads` table directly via the Supabase JS client (using the anon key). The FastAPI backend is used for exports, sync logs, call-outcome recording, and physician-level queries.

---

## Repository Structure

```
physician-lead-system/
├── backend/                    # Python FastAPI backend + ETL pipeline
│   ├── api/
│   │   ├── main.py             # FastAPI app entry point
│   │   ├── routes/
│   │   │   ├── physicians.py   # Physician query endpoints
│   │   │   └── leads.py        # Lead management endpoints
│   │   └── schemas/
│   │       └── physician_schema.py
│   ├── etl/
│   │   ├── ingest_nppes.py     # Main ETL — reads NPPES CSV, upserts DB
│   │   ├── validate_records.py # Pure validation functions (NPI, names, etc.)
│   │   ├── compute_scores.py   # 4-pillar scoring + contact_category logic
│   │   ├── download_nppes.py   # Finds the latest NPPES file in raw_data/
│   │   ├── enrich_phones.py    # Phone enrichment — PDL + Twilio + Telnyx
│   │   ├── enrich_emails.py    # Email enrichment — Hunter.io
│   │   ├── enrich_contactout.py
│   │   ├── enrich_fullenrich.py
│   │   └── enrich_contactout_linkedin.py
│   ├── models/
│   │   ├── physician.py        # Central physician record (~80 columns)
│   │   ├── practice_location.py
│   │   ├── license.py
│   │   ├── organization.py
│   │   ├── enrichment_source.py
│   │   └── metadata.py
│   ├── migrations/             # Alembic migration scripts
│   ├── reference_data/         # NUCC taxonomy, ZIP-state, license rules
│   ├── raw_data/nppes/         # Drop NPPES CSV files here
│   ├── tests/                  # pytest test suite (54 tests)
│   ├── database.py             # SQLAlchemy engine + session factory
│   ├── scheduler.py            # Weekly sync scheduler (Sunday 02:00)
│   ├── requirements.txt
│   └── .env                    # Secrets — never commit this file
│
├── frontend/                   # React + TypeScript frontend
│   ├── src/
│   │   ├── pages/              # Dashboard, Leads, Activity
│   │   ├── components/         # UI components (table, drawer, modal, etc.)
│   │   ├── hooks/              # useLeads, useStats, useReviews
│   │   ├── lib/                # Supabase client, utils
│   │   └── types/              # Shared TypeScript interfaces
│   ├── package.json
│   └── .env.local              # Supabase URL + anon key — never commit
│
└── README.md                   # This file
```

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Backend runtime |
| Node.js | 20+ | Frontend build |
| npm | 10+ | Frontend package manager |
| Supabase account | — | Hosted PostgreSQL database |

You do **not** need a local PostgreSQL installation. The database is hosted on Supabase. You need:
- The Supabase project `DATABASE_URL` (connection string) for the backend
- The Supabase project URL and anon key for the frontend

---

## Quick Start

### 1. Get the credentials from your team lead

You need two credential sets before anything will work:

**Backend** (`backend/.env`):
```
DATABASE_URL
HUNTER_API_KEY
PDL_API_KEY
TWILIO_ACCOUNT_SID
TWILIO_AUTH_TOKEN
TELNYX_API_KEY
CONTACTOUT_API_KEY
FULLENRICH_API_KEY
LUSHA_API_KEY
```

**Frontend** (`frontend/.env.local`):
```
VITE_SUPABASE_URL
VITE_SUPABASE_ANON_KEY
```

### 2. Set up the backend

```bash
cd backend
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

pip install -r requirements.txt
```

Create `backend/.env` with the credentials above, then run:

```bash
# Apply all database migrations
alembic upgrade head

# Load static reference data (run once, or after a schema reset)
python reference_data/load_reference_data.py
```

### 3. Set up the frontend

```bash
cd frontend
npm install
```

Create `frontend/.env.local` with the Supabase credentials above.

### 4. Run both services

In one terminal:
```bash
cd backend
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

In a second terminal:
```bash
cd frontend
npm run dev
```

The app will be at **http://localhost:5173**. API docs at **http://localhost:8000/docs**.

---

## Environment Variables

### Backend — `backend/.env`

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | **Yes** | Supabase PostgreSQL connection string. Use the `postgresql://` format — the app converts it to `postgresql+psycopg://` automatically. |
| `HUNTER_API_KEY` | For email enrichment | Hunter.io API key |
| `PDL_API_KEY` | For phone enrichment | PeopleDataLabs API key |
| `TWILIO_ACCOUNT_SID` | For phone enrichment | Twilio account SID |
| `TWILIO_AUTH_TOKEN` | For phone enrichment | Twilio auth token |
| `TELNYX_API_KEY` | For DNC checking | Telnyx API key |
| `CONTACTOUT_API_KEY` | For email enrichment | ContactOut API key |
| `FULLENRICH_API_KEY` | For email enrichment | FullEnrich API key |
| `LUSHA_API_KEY` | For email enrichment | Lusha API key |

The API and ETL will start without the enrichment keys — they are only needed when you run the enrichment scripts.

### Frontend — `frontend/.env.local`

| Variable | Required | Description |
|----------|----------|-------------|
| `VITE_SUPABASE_URL` | **Yes** | Your Supabase project URL (e.g. `https://xxxx.supabase.co`) |
| `VITE_SUPABASE_ANON_KEY` | **Yes** | Supabase anon (public) key |

> **Note:** The Supabase anon key is intentionally public-facing. Access is controlled by Row Level Security (RLS) policies on the database side.

---

## How the Data Pipeline Works

### Step 1 — Download NPPES data

Download the full NPI data file from the [CMS NPPES download page](https://download.cms.gov/nppes/NPI_Files.html). The file is named `npidata_pfile_YYYYMMDD-YYYYMMDD.csv`. Place it in `backend/raw_data/nppes/`.

### Step 2 — Run the ETL

```bash
cd backend

# Process the full file (takes ~30–60 minutes for the full 8M row dataset)
python etl/ingest_nppes.py

# Test with a subset first
python etl/ingest_nppes.py --limit 5000

# Point at a specific file
python etl/ingest_nppes.py --file raw_data/nppes/npidata_pfile_20050523-20260308.csv
```

This script:
- Reads the CSV in 50,000-row chunks
- Filters to individual physicians (Entity Type 1) only
- Validates and normalises every field (NPI Luhn check, name normalisation, address confidence scoring, license validation)
- Computes the 4-pillar lead score (0–100) for each physician
- Upserts into `physician`, `license`, and `physician_practice_locations` tables

### Step 3 — Run enrichment

Enrichment populates `mobile_phone` and `personal_email` on the physician table, then syncs qualifying records into the `leads` table.

```bash
# Phone enrichment (PeopleDataLabs → Twilio → Telnyx DNC)
python etl/enrich_phones.py              # all unattempted physicians
python etl/enrich_phones.py --limit 50  # batch of 50
python etl/enrich_phones.py --npi 1234567890  # single physician

# Email enrichment (Hunter.io)
python etl/enrich_emails.py --limit 25
python etl/enrich_emails.py --npi 1234567890
```

Each enrichment script:
1. Queries the `physician` table for records not yet attempted by that source
2. Calls the external API
3. Writes results to `personal_email` / `mobile_phone` on the physician
4. Appends the source name to `physician.enrichment_sources[]`
5. Atomically increments stats in `enrichment_source_stats`
6. Calls `sync_to_leads()` to upsert the physician into the `leads` table

### Step 4 — Weekly automation

```bash
python scheduler.py          # runs every Sunday at 02:00
python scheduler.py --now    # run immediately
```

---

## Lead Scoring Model

Every physician gets a score from **0 to 100** across four pillars:

| Pillar | Max Points | What it measures |
|--------|-----------|-----------------|
| **Reachability** | 40 | Phone (confidence + DNC-clear) + email (confidence) |
| **Practice Structure** | 25 | Solo practice scores highest; large hospital systems score 0 |
| **Activity / Validity** | 20 | Active NPI, valid license, recent NPPES update, graduation year present |
| **Target Fit** | 15 | Target specialty, mid-career experience (5–20 yrs), target state, multi-state license |

**Score-based tiers:**

| Tier | Score Range | Priority |
|------|------------|---------|
| A | 80–100 | High — prioritise for outreach |
| B | 60–79 | Medium |
| C | 40–59 | Low |
| Archive | < 40 | Excluded from active pipeline |

---

## Contact Categories

**This is separate from lead tier and is the primary gate for the `leads` table.**

| Category | Condition | Meaning |
|----------|-----------|---------|
| A | Mobile phone present + any email present | Ready for direct outreach |
| B | Any email present, no phone | Email outreach only |
| EXCLUDED | No contact info at all | Never appears in leads table |

A physician scoring 100 points with no email and no phone is **EXCLUDED** from the leads table entirely. A physician scoring 40 points with a verified mobile phone and email is **Category A**. Score determines ordering within categories, not whether a physician appears at all.

---

## Running the Services

### API

```bash
cd backend
uvicorn api.main:app --reload --port 8000
```

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Database connectivity check |
| `/api/v1/physicians/` | GET | List physicians with filters |
| `/api/v1/physicians/stats` | GET | Aggregate database stats |
| `/api/v1/physicians/{npi}` | GET | Single physician by NPI |
| `/api/v1/leads/` | GET | Paginated lead list |
| `/api/v1/leads/export` | GET | Download leads as CSV |
| `/api/v1/leads/sync-logs` | GET | ETL run history |
| `/api/v1/leads/{npi}/call-outcome` | POST | Record a call result |
| `/api/v1/leads/pipeline/summary` | GET | Dashboard aggregate stats |

Full interactive docs: **http://localhost:8000/docs**

### Frontend

```bash
cd frontend
npm run dev        # development server (http://localhost:5173)
npm run build      # production build into dist/
npm run preview    # preview production build locally
npm run lint       # ESLint check
```

### Tests

```bash
cd backend
pytest tests/ -v
```

54 tests covering NPI Luhn validation, name normalisation, address validation, license validation, experience calculation, gender normalisation, and lead scoring.

---

## Key Concepts to Understand First

Before diving into the code, make sure you understand these:

**1. `physician` vs `leads` table**
The `physician` table is the master record (one row per NPI, ~80 columns). The `leads` table is a denormalised flat copy containing only physicians who have at least one contact signal. The frontend reads `leads` directly via Supabase. The `physician` table is written by the ETL and enrichment scripts.

**2. `lead_tier` vs `contact_category`**
`lead_tier` (A/B/C/Archive) is score-based. `contact_category` (A/B/EXCLUDED) is contact-based. Both exist. The UI primarily filters by `contact_category`. See `etl/compute_scores.py` for the authoritative definition.

**3. Email column convention**
There are three email fields on `physician`:
- `personal_email` — canonical write target for all enrichment tools (Hunter v2+, Apollo, ContactOut, etc.)
- `practice_email` — sourced from NPPES (less reliable for outreach)
- `email` — **legacy read-only** Hunter v1 data; kept to avoid data loss

All new enrichment scripts write to `personal_email`. Display logic reads `personal_email` first and falls back to `email`.

**4. The `enrichment_sources` array**
`physician.enrichment_sources` is an append-only PostgreSQL text array (e.g. `["hunter.io", "fullenrich"]`). Each enrichment script appends its source name when it successfully processes a physician. This prevents re-processing and enables per-source deduplication.

**5. TCPA compliance**
`phone_state_risk` on the physician record reflects state-level calling laws (CRITICAL/HIGH/MEDIUM). CA and FL are CRITICAL — explicit consent is required before dialling mobile numbers. This is set by `compute_scores.get_state_risk()` during phone enrichment.
