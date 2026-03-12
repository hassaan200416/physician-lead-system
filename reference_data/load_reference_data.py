# reference_data/load_reference_data.py
# Loads all reference CSV files into the database.
# Run this ONCE after initial setup.
# Safe to re-run — uses upsert logic to avoid duplicates.

import os
import sys
import csv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database import engine
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def create_reference_tables():
    """Creates reference tables if they do not exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS nucc_taxonomy_reference (
                taxonomy_code   VARCHAR(20) PRIMARY KEY,
                taxonomy_type   VARCHAR(100),
                classification  VARCHAR(100),
                specialization  VARCHAR(100),
                is_physician    BOOLEAN DEFAULT FALSE,
                campaign_bucket VARCHAR(50)
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS excluded_taxonomy_codes (
                taxonomy_code VARCHAR(20) PRIMARY KEY,
                reason        VARCHAR(200)
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS source_registry (
                source_name      VARCHAR(50) PRIMARY KEY,
                source_type      VARCHAR(20),
                authority_rank   SMALLINT,
                update_frequency VARCHAR(30),
                last_ingested    TIMESTAMPTZ
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS zip_state_reference (
                zip_code   CHAR(5) PRIMARY KEY,
                state_code CHAR(2) NOT NULL
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS license_format_rules (
                state_code    CHAR(2) PRIMARY KEY,
                pattern_regex VARCHAR(100),
                min_length    SMALLINT,
                max_length    SMALLINT,
                example       VARCHAR(30)
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sync_log (
                sync_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                sync_started_at     TIMESTAMPTZ NOT NULL,
                sync_completed_at   TIMESTAMPTZ,
                source_file         VARCHAR(300),
                records_processed   INTEGER DEFAULT 0,
                records_inserted    INTEGER DEFAULT 0,
                records_updated     INTEGER DEFAULT 0,
                records_deactivated INTEGER DEFAULT 0,
                records_failed      INTEGER DEFAULT 0,
                error_rate_pct      DECIMAL(5,2),
                status              VARCHAR(20),
                notes               TEXT
            )
        """))

        conn.commit()
        print("Reference tables created or verified")


def load_nucc_taxonomy():
    csv_path = os.path.join(BASE_DIR, "nucc_taxonomy.csv")
    count = 0
    with engine.connect() as conn:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                is_physician = row["is_physician"].strip().lower() == "true"
                conn.execute(text("""
                    INSERT INTO nucc_taxonomy_reference
                        (taxonomy_code, taxonomy_type, classification,
                         specialization, is_physician, campaign_bucket)
                    VALUES
                        (:code, :type, :classification,
                         :specialization, :is_physician, :bucket)
                    ON CONFLICT (taxonomy_code) DO UPDATE SET
                        taxonomy_type   = EXCLUDED.taxonomy_type,
                        classification  = EXCLUDED.classification,
                        specialization  = EXCLUDED.specialization,
                        is_physician    = EXCLUDED.is_physician,
                        campaign_bucket = EXCLUDED.campaign_bucket
                """), {
                    "code": row["taxonomy_code"].strip(),
                    "type": row["taxonomy_type"].strip(),
                    "classification": row["classification"].strip(),
                    "specialization": row["specialization"].strip() or None,
                    "is_physician": is_physician,
                    "bucket": row["campaign_bucket"].strip() or None,
                })
                count += 1
        conn.commit()
    print(f"Loaded {count} taxonomy codes")


def load_excluded_taxonomies():
    csv_path = os.path.join(BASE_DIR, "excluded_taxonomies.csv")
    count = 0
    with engine.connect() as conn:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(text("""
                    INSERT INTO excluded_taxonomy_codes
                        (taxonomy_code, reason)
                    VALUES (:code, :reason)
                    ON CONFLICT (taxonomy_code) DO UPDATE SET
                        reason = EXCLUDED.reason
                """), {
                    "code": row["taxonomy_code"].strip(),
                    "reason": row["reason"].strip(),
                })
                count += 1
        conn.commit()
    print(f"Loaded {count} excluded taxonomy codes")


def load_source_registry():
    csv_path = os.path.join(BASE_DIR, "source_registry.csv")
    count = 0
    with engine.connect() as conn:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(text("""
                    INSERT INTO source_registry
                        (source_name, source_type,
                         authority_rank, update_frequency)
                    VALUES
                        (:name, :type, :rank, :freq)
                    ON CONFLICT (source_name) DO UPDATE SET
                        source_type      = EXCLUDED.source_type,
                        authority_rank   = EXCLUDED.authority_rank,
                        update_frequency = EXCLUDED.update_frequency
                """), {
                    "name": row["source_name"].strip(),
                    "type": row["source_type"].strip(),
                    "rank": int(row["authority_rank"].strip()),
                    "freq": row["update_frequency"].strip(),
                })
                count += 1
        conn.commit()
    print(f"Loaded {count} source registry entries")


def load_zip_state_reference():
    csv_path = os.path.join(BASE_DIR, "zip_state_reference.csv")
    if not os.path.exists(csv_path):
        print("zip_state_reference.csv not found — skipping")
        return
    count = 0
    batch: list[dict[str, str]] = []
    batch_size = 1000
    with engine.connect() as conn:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append({
                    "zip": row["zip_code"].strip(),
                    "state": row["state_code"].strip(),
                })
                if len(batch) >= batch_size:
                    conn.execute(text("""
                        INSERT INTO zip_state_reference
                            (zip_code, state_code)
                        VALUES (:zip, :state)
                        ON CONFLICT (zip_code) DO NOTHING
                    """), batch)
                    count += len(batch)
                    batch = []
            if batch:
                conn.execute(text("""
                    INSERT INTO zip_state_reference
                        (zip_code, state_code)
                    VALUES (:zip, :state)
                    ON CONFLICT (zip_code) DO NOTHING
                """), batch)
                count += len(batch)
        conn.commit()
    print(f"Loaded {count} ZIP codes")


def load_license_format_rules():
    """
    Basic license format rules for common states.
    Can be expanded later by adding rows directly to the database.
    """
    rules = [
        ("TX", r"^[A-Z]{1}[0-9]{4,6}$", 5, 7, "G12345"),
        ("CA", r"^[A-Z]{1}[0-9]{5,6}$", 6, 7, "A123456"),
        ("FL", r"^(ME|DO|OS)[0-9]{6,7}$", 8, 9, "ME123456"),
        ("NY", r"^[0-9]{6}$", 6, 6, "123456"),
        ("PA", r"^MD[0-9]{6}$", 8, 8, "MD123456"),
        ("IL", r"^[0-9]{6,8}$", 6, 8, "12345678"),
        ("OH", r"^[0-9]{6}$", 6, 6, "123456"),
        ("GA", r"^[0-9]{6}$", 6, 6, "123456"),
        ("NC", r"^[0-9]{5}$", 5, 5, "12345"),
        ("MI", r"^[0-9]{6}$", 6, 6, "123456"),
    ]
    count = 0
    with engine.connect() as conn:
        for state, pattern, min_len, max_len, example in rules:
            conn.execute(text("""
                INSERT INTO license_format_rules
                    (state_code, pattern_regex,
                     min_length, max_length, example)
                VALUES
                    (:state, :pattern, :min_len, :max_len, :example)
                ON CONFLICT (state_code) DO UPDATE SET
                    pattern_regex = EXCLUDED.pattern_regex,
                    min_length    = EXCLUDED.min_length,
                    max_length    = EXCLUDED.max_length,
                    example       = EXCLUDED.example
            """), {
                "state": state, "pattern": pattern,
                "min_len": min_len, "max_len": max_len,
                "example": example,
            })
            count += 1
        conn.commit()
    print(f"Loaded {count} license format rules")


if __name__ == "__main__":
    print("Loading reference data into database...")
    print("-" * 40)
    create_reference_tables()
    load_nucc_taxonomy()
    load_excluded_taxonomies()
    load_source_registry()
    load_zip_state_reference()
    load_license_format_rules()
    print("-" * 40)
    print("All reference data loaded successfully")
