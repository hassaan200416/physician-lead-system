# Physician Lead Generation System

A production-grade backend pipeline that collects, validates, scores, and serves US physician lead data for AI-powered outreach campaigns.

## Architecture

- **Data Source**: CMS NPPES weekly bulk download (free, federal, authoritative)
- **Database**: PostgreSQL via Supabase
- **Backend**: FastAPI + SQLAlchemy
- **ETL**: Python + pandas, chunked processing
- **Scheduler**: Weekly automated sync

## Stack

| Tool        | Purpose        |
| ----------- | -------------- |
| Python 3.11 | Runtime        |
| FastAPI     | REST API       |
| SQLAlchemy  | ORM            |
| Alembic     | Migrations     |
| pandas      | CSV processing |
| PostgreSQL  | Database       |
| Supabase    | Cloud hosting  |
| pytest      | Testing        |

## Project Structure

```
physician-lead-system/
|- etl/                    # ETL pipeline
|  |- ingest_nppes.py      # Main ETL script
|  |- validate_records.py  # Field validation
|  |- compute_scores.py    # Lead scoring
|  |- download_nppes.py    # File management
|- models/                 # SQLAlchemy models
|- api/                    # FastAPI application
|  |- main.py
|  |- routes/
|  |- schemas/
|- reference_data/         # Taxonomy, ZIP, license rules
|- migrations/             # Alembic migrations
|- tests/                  # pytest test suite
|- database.py             # DB connection
|- scheduler.py            # Weekly sync scheduler
```

## Setup

1. Clone the repository
2. Create virtual environment: `py -3.11 -m venv venv`
3. Activate: `venv\Scripts\activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and fill in your Supabase credentials
6. Run migrations: `alembic upgrade head`
7. Load reference data: `python reference_data/load_reference_data.py`
8. Download NPPES file from https://download.cms.gov/nppes/NPI_Files.html
9. Run ETL: `python etl/ingest_nppes.py --file path/to/npidata_pfile.csv`

## Running the API

```bash
uvicorn api.main:app --reload --port 8000
```

API docs available at: http://127.0.0.1:8000/docs

## API Endpoints

### Physicians

- `GET /api/v1/physicians/` - List physicians with filters
- `GET /api/v1/physicians/stats` - Database statistics
- `GET /api/v1/physicians/{npi}` - Get physician by NPI

### Leads

- `GET /api/v1/leads/export` - Export leads as CSV
- `GET /api/v1/leads/sync-logs` - ETL sync history
- `POST /api/v1/leads/{npi}/call-outcome` - Record call result
- `GET /api/v1/leads/pipeline/summary` - Pipeline dashboard

## Running Tests

```bash
pytest tests/ -v
```

54 tests covering NPI validation, name normalization, address validation, license validation, experience calculation, gender normalization, and lead scoring.

## Lead Scoring

Scores are 0-100 across 4 pillars:

| Pillar              | Max Points | Description                      |
| ------------------- | ---------- | -------------------------------- |
| Reachability        | 40         | Phone + email verification       |
| Practice Structure  | 25         | Solo vs hospital system          |
| Activity & Validity | 20         | Active NPI, valid license        |
| Target Fit          | 15         | Specialty, experience, geography |

Tiers: A (80-100), B (60-79), C (40-59), Archive (<40)

## Weekly Sync

```bash
python scheduler.py          # Start scheduled weekly sync
python scheduler.py --now    # Run sync immediately
```

## Adding Phone and Email (Future)

Phone and email pipelines plug directly into the existing schema. The `physician` table has reserved columns and the scoring engine has Pillar 1 (Reachability) sitting at 0 ready to be filled. No schema changes required.
