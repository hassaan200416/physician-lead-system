# api/routes/leads.py
# Lead management endpoints.
# Exports, filters, sync logs, and call feedback.

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Any, Optional
import csv
import io
from fastapi.responses import StreamingResponse

from database import get_db
from api.schemas.physician_schema import SyncLogResponse

router = APIRouter()


@router.get("")
def get_leads(
    tier: Optional[str] = Query(None),
    contact_completeness: Optional[str] = Query(None),
    include_uncontactable: bool = Query(False),
    state: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    Returns leads with contact completeness fields and pagination.

    Filters:
    - tier: A / B / C
    - contact_completeness: EXCELLENT / GOOD / PARTIAL / UNCONTACTABLE
    - include_uncontactable: False by default
    - state: two-letter state code
    """
    filters: list[str] = []
    params: dict[str, Any] = {"skip": skip, "limit": limit}

    if tier:
        filters.append("lead_tier = :tier")
        params["tier"] = tier.upper()

    if contact_completeness:
        filters.append("contact_completeness = :contact_completeness")
        params["contact_completeness"] = contact_completeness.upper()

    if not include_uncontactable:
        filters.append("COALESCE(is_uncontactable, FALSE) = FALSE")

    if state:
        filters.append("state = :state")
        params["state"] = state.upper()

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    total_row = db.execute(
        text(f"SELECT COUNT(*) FROM leads {where_clause}"),
        params,
    ).fetchone()
    total = int(total_row[0]) if total_row and total_row[0] is not None else 0

    rows = db.execute(
        text(
            f"""
            SELECT *
            FROM leads
            {where_clause}
            ORDER BY lead_score DESC NULLS LAST
            OFFSET :skip
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()

    return {
        "total": total,
        "leads": [dict(row) for row in rows],
        "skip": skip,
        "limit": limit,
    }


@router.get("/export")
def export_leads(
    tier: Optional[str] = Query(None, description="Filter by tier: A, B, C"),
    state: Optional[str] = Query(None, description="Filter by state"),
    specialty_category: Optional[str] = Query(None),
    min_score: Optional[float] = Query(None),
    limit: int = Query(500, le=5000),
    db: Session = Depends(get_db)
) -> StreamingResponse:
    """
    Exports physician leads as a downloadable CSV file.

    Applies the same filter logic as list_physicians but
    returns a streaming CSV response instead of JSON.
    Joins with primary practice location for address fields.

    Only active physicians with at least one address are included.
    Results ordered by lead_score_current DESC.

    Args:
        tier:               Filter by lead tier ('A', 'B', 'C').
        state:              Filter by state code.
        specialty_category: Filter by NUCC campaign bucket.
        min_score:          Minimum lead score to include.
        limit:              Max records to export. Hard cap 5000.

    Returns:
        StreamingResponse — CSV file download named
        'physician_leads.csv' with 18 columns.
    """
    filters = ["p.is_active = TRUE"]
    params: dict[str, Any] = {}

    if tier:
        filters.append("p.lead_tier = :tier")
        params["tier"] = tier.upper()

    if state:
        filters.append("""
            EXISTS (
                SELECT 1 FROM physician_practice_locations pl
                WHERE pl.npi = p.npi AND pl.state = :state
            )
        """)
        params["state"] = state.upper()

    if specialty_category:
        filters.append("p.derived_specialty_category = :specialty_category")
        params["specialty_category"] = specialty_category

    if min_score is not None:
        filters.append("p.lead_score_current >= :min_score")
        params["min_score"] = min_score

    where_clause = "WHERE " + " AND ".join(filters)
    params["limit"] = limit

    rows = db.execute(text(f"""
        SELECT
            p.npi,
            p.full_name_display,
            p.first_name_clean,
            p.last_name_clean,
            p.credential_normalized,
            p.specialty_name,
            p.derived_specialty_category,
            p.experience_bucket,
            p.years_of_experience,
            p.lead_score_current,
            p.lead_tier,
            p.email_pattern_name,
            p.multi_state_flag,
            p.license_count,
            pl.address_line_1,
            pl.city,
            pl.state,
            pl.zip
        FROM physician p
        LEFT JOIN physician_practice_locations pl
            ON p.npi = pl.npi AND pl.is_primary_location = TRUE
        {where_clause}
        ORDER BY p.lead_score_current DESC NULLS LAST
        LIMIT :limit
    """), params).fetchall()

    # Build CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "NPI", "Full Name", "First Name", "Last Name",
        "Credential", "Specialty", "Specialty Category",
        "Experience Bucket", "Years Experience",
        "Lead Score", "Lead Tier",
        "Email Pattern", "Multi State", "License Count",
        "Address", "City", "State", "ZIP"
    ])

    for row in rows:
        writer.writerow([
            row[0], row[1], row[2], row[3],
            row[4], row[5], row[6],
            row[7], row[8],
            row[9], row[10],
            row[11], row[12], row[13],
            row[14], row[15], row[16], row[17]
        ])

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=physician_leads.csv"
        }
    )


@router.get("/sync-logs", response_model=list[SyncLogResponse])
def get_sync_logs(
    limit: int = Query(10, le=50),
    db: Session = Depends(get_db)
) -> list[SyncLogResponse]:
    """
    Returns the most recent ETL sync log entries.

    Each entry represents one full run of ingest_nppes.py,
    including record counts, error rate, duration, and status.
    Useful for monitoring pipeline health and diagnosing issues.

    Args:
        limit: Number of recent logs to return. Max 50. Default 10.

    Returns:
        List of SyncLogResponse ordered by most recent first.
    """
    rows = db.execute(text("""
        SELECT
            sync_id::text, sync_started_at, sync_completed_at,
            source_file, records_processed, records_inserted,
            records_updated, records_failed,
            error_rate_pct, status, notes
        FROM sync_log
        ORDER BY sync_started_at DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return [
        SyncLogResponse(
            sync_id=r[0],
            sync_started_at=r[1],
            sync_completed_at=r[2],
            source_file=r[3],
            records_processed=r[4],
            records_inserted=r[5],
            records_updated=r[6],
            records_failed=r[7],
            error_rate_pct=float(r[8]) if r[8] else None,
            status=r[9],
            notes=r[10],
        )
        for r in rows
    ]


@router.post("/{npi}/call-outcome")
def record_call_outcome(
    npi: str,
    outcome: str = Query(
        ...,
        description="Call outcome: answered, voicemail, wrong_number, do_not_call, interested, not_interested"
    ),
    db: Session = Depends(get_db)
) -> dict[str, str]:
    """
    Records the outcome of a call attempt for a physician.

    Increments call_attempt_count, stores the outcome string,
    and updates last_called_at. If outcome is 'do_not_call',
    applies a -20 point score penalty and recalculates tier.

    Valid outcomes:
        answered         — physician picked up
        voicemail        — reached voicemail
        wrong_number     — number does not belong to physician
        do_not_call      — physician requested no contact (-20pts)
        interested       — physician expressed interest
        not_interested   — physician declined

    Args:
        npi:     Physician's NPI.
        outcome: One of the valid outcome strings above.

    Returns:
        Dict with npi, outcome_recorded, and confirmation message.

    Raises:
        400: If outcome is not one of the valid values.
        404: If physician NPI not found.
    """
    valid_outcomes = {
        "answered", "voicemail", "wrong_number",
        "do_not_call", "interested", "not_interested"
    }

    if outcome not in valid_outcomes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid outcome. Must be one of: {', '.join(valid_outcomes)}"
        )

    # Check physician exists
    exists = db.execute(
        text("SELECT npi FROM physician WHERE npi = :npi"),
        {"npi": npi}
    ).fetchone()

    if not exists:
        raise HTTPException(status_code=404, detail=f"Physician {npi} not found")

    # Update call tracking
    db.execute(text("""
        UPDATE physician SET
            last_call_outcome = :outcome,
            call_attempt_count = COALESCE(call_attempt_count, 0) + 1,
            last_called_at = NOW(),
            updated_at = NOW()
        WHERE npi = :npi
    """), {"npi": npi, "outcome": outcome})

    # Apply score adjustment for do_not_call
    if outcome == "do_not_call":
        db.execute(text("""
            UPDATE physician SET
                lead_score_current = GREATEST(0, lead_score_current - 20),
                lead_tier = CASE
                    WHEN lead_score_current - 20 >= 80 THEN 'A'
                    WHEN lead_score_current - 20 >= 60 THEN 'B'
                    WHEN lead_score_current - 20 >= 40 THEN 'C'
                    ELSE 'Archive'
                END
            WHERE npi = :npi
        """), {"npi": npi})

    db.commit()

    return {
        "npi": npi,
        "outcome_recorded": outcome,
        "message": "Call outcome recorded successfully"
    }


@router.get("/pipeline/summary")
def pipeline_summary(db: Session = Depends(get_db)) -> dict[str, Any]:
    """
    Returns a high-level summary of the lead pipeline.

    Aggregates tier distribution, experience breakdown, top states,
    and last sync status into a single response for manager reporting.

    Returns:
        Dict with:
            tier_breakdown:       List of {tier, count, avg_score}
            experience_breakdown: List of {bucket, count}
            top_states:           Top 10 states by physician count
            last_sync:            Most recent ETL run metadata
    """
    tiers = db.execute(text("""
        SELECT
            lead_tier,
            COUNT(*) as count,
            ROUND(AVG(lead_score_current), 1) as avg_score
        FROM physician
        WHERE is_active = TRUE AND lead_tier IS NOT NULL
        GROUP BY lead_tier
        ORDER BY
            CASE lead_tier
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
                WHEN 'Archive' THEN 4
            END
    """)).fetchall()

    experience = db.execute(text("""
        SELECT experience_bucket, COUNT(*)
        FROM physician
        WHERE is_active = TRUE AND experience_bucket IS NOT NULL
        GROUP BY experience_bucket
    """)).fetchall()

    top_states = db.execute(text("""
        SELECT pl.state, COUNT(DISTINCT p.npi) as count
        FROM physician p
        JOIN physician_practice_locations pl ON p.npi = pl.npi
        WHERE p.is_active = TRUE AND pl.state IS NOT NULL
        GROUP BY pl.state
        ORDER BY count DESC
        LIMIT 10
    """)).fetchall()

    last_sync = db.execute(text("""
        SELECT sync_completed_at, records_inserted,
               records_updated, status
        FROM sync_log
        ORDER BY sync_started_at DESC
        LIMIT 1
    """)).fetchone()

    return {
        "tier_breakdown": [
            {"tier": r[0], "count": r[1], "avg_score": float(r[2]) if r[2] else 0}
            for r in tiers
        ],
        "experience_breakdown": [
            {"bucket": r[0], "count": r[1]}
            for r in experience
        ],
        "top_states": [
            {"state": r[0], "count": r[1]}
            for r in top_states
        ],
        "last_sync": {
            "completed_at": last_sync[0] if last_sync else None,
            "records_inserted": last_sync[1] if last_sync else None,
            "records_updated": last_sync[2] if last_sync else None,
            "status": last_sync[3] if last_sync else None,
        } if last_sync else None,
    }
