"""Add enrichment source tracking and contact category system.

Revision ID : 003_enrichment_sources_and_stats
Revises     : 002_phone_enrichment
Create Date : 2026-03-28

What this migration does
------------------------
1. physician table
   - Adds personal_email + personal_email_confidence
     (canonical write target for all new enrichment sources)
   - Adds email_enriched_at
     (timestamp of most recent successful email enrichment)
   - Adds enrichment_sources[]
     (append-only list of tool names that contributed data)
   - Adds enrichment_last_attempted_at
     (scheduler helper - skip recently processed records)
   - SKIPS email_enrichment_attempted - already exists in DB

   Legacy columns kept as-is (read-only going forward):
     email, email_confidence_score, email_confidence_level,
     email_verification_status, email_source, email_acquired_at,
     email_enrichment_result
   These are never dropped - Hunter v1 data lives there.
   New enrichment writes to personal_email instead.

2. leads table
   - Adds enrichment_sources[]
     (mirrors physician - denormalised for fast frontend queries)
   - Adds contact_category
     (A / B - contact-based tier, EXCLUDED rows never inserted)

   Contact category rules:
     A = mobile_phone present AND any email present
     B = any email present but no mobile_phone
     EXCLUDED = no contact info - row never inserted into leads

3. enrichment_source_stats table (new)
   One row per enrichment tool. Updated atomically after each tool run.
   Hit rate = total_hits / total_attempts.
   Seeded with one row per known source on creation.

Trade-offs recorded
-------------------
- personal_email is the canonical write target for all sources added
  after Hunter v1. During transition, enrich_emails.py writes to both
  email (legacy) and personal_email so existing queries keep working.

- enrichment_sources is TEXT[] not a join table. Simple and fast at
  this data volume. Convert to normalised enrichment_runs table if
  source count grows beyond ~20 or query patterns change.

- contact_category is denormalised into leads so the frontend can
  filter without joining back to physician.

- total_attempts counts every physician a tool was called for, hit or
  miss. Enables hit-rate calculation per source.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "003_enrichment_sources_and_stats"
down_revision = "002_phone_enrichment"
branch_labels = None
depends_on = None


def upgrade() -> None:

    # -- 1. physician - new enrichment columns --
    # IMPORTANT: email_enrichment_attempted already exists in DB - skipped.

    op.add_column("physician", sa.Column(
        "personal_email", sa.String(255), nullable=True,
        comment="Canonical personal email from enrichment tools. "
                "All sources after Hunter v1 write here. "
                "Legacy 'email' column kept read-only for backward compat."
    ))
    op.add_column("physician", sa.Column(
        "personal_email_confidence", sa.String(10), nullable=True,
        comment="Confidence level for personal_email: HIGH | MEDIUM | LOW."
    ))
    op.add_column("physician", sa.Column(
        "email_enriched_at", sa.DateTime(timezone=True), nullable=True,
        comment="Timestamp of the most recent successful email enrichment "
                "from any source."
    ))
    op.add_column("physician", sa.Column(
        "enrichment_sources",
        postgresql.ARRAY(sa.Text()),
        server_default=sa.text("'{}'::text[]"),
        nullable=False,
        comment="Ordered append-only list of tool names that contributed data. "
                "Values must match enrichment_source_stats.source_name exactly. "
                "Example: [\"hunter.io\", \"apollo\"]."
    ))
    op.add_column("physician", sa.Column(
        "enrichment_last_attempted_at", sa.DateTime(timezone=True), nullable=True,
        comment="Last time any enrichment tool attempted this physician. "
                "Schedulers use this to skip recently processed records."
    ))

    op.create_index(
        "idx_physician_enrichment_sources",
        "physician",
        ["enrichment_sources"],
        postgresql_using="gin",
    )

    # -- 2. leads - enrichment mirror + contact_category --

    op.add_column("leads", sa.Column(
        "enrichment_sources",
        postgresql.ARRAY(sa.Text()),
        server_default=sa.text("'{}'::text[]"),
        nullable=False,
        comment="Mirrors physician.enrichment_sources. Denormalised for fast "
                "frontend filtering without a join."
    ))
    op.add_column("leads", sa.Column(
        "contact_category", sa.String(10), nullable=True,
        comment="Contact-based lead tier. "
                "A = mobile_phone + any email. "
                "B = any email, no mobile_phone. "
                "Rows with no contact info are never inserted. "
                "Recomputed on every contact field update."
    ))

    op.create_index("idx_leads_contact_category", "leads", ["contact_category"])
    op.create_index(
        "idx_leads_enrichment_sources", "leads",
        ["enrichment_sources"], postgresql_using="gin"
    )

    # -- 3. enrichment_source_stats - new table --

    op.create_table(
        "enrichment_source_stats",
        sa.Column(
            "source_name", sa.String(50), primary_key=True, nullable=False,
            comment="Canonical tool id. Must match enrichment_sources array values."
        ),
        sa.Column(
            "emails_provided", sa.Integer(), server_default="0", nullable=False,
            comment="Cumulative personal emails delivered by this source."
        ),
        sa.Column(
            "phones_provided", sa.Integer(), server_default="0", nullable=False,
            comment="Cumulative mobile phones delivered by this source."
        ),
        sa.Column(
            "total_hits", sa.Integer(), server_default="0", nullable=False,
            comment="Physicians where this source returned any data. "
                    "Hit rate = total_hits / total_attempts."
        ),
        sa.Column(
            "total_attempts", sa.Integer(), server_default="0", nullable=False,
            comment="Total physicians this source was called for, hit or miss."
        ),
        sa.Column(
            "last_used_at", sa.DateTime(timezone=True), nullable=True,
            comment="Timestamp of most recent completed run. Null if never run."
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("now()"), nullable=False,
            comment="Application must update this on every stats write."
        ),
    )

    # Seed one row per known source - stats tracking ready before first run.
    op.execute("""
        INSERT INTO enrichment_source_stats (source_name) VALUES
            ('hunter.io'),
            ('contactout'),
            ('apollo'),
            ('fullenrich'),
            ('lusha'),
            ('snov.io'),
            ('rocketreach'),
            ('bettercontact')
        ON CONFLICT (source_name) DO NOTHING;
    """)


def downgrade() -> None:

    op.drop_table("enrichment_source_stats")

    op.drop_index("idx_leads_contact_category", table_name="leads")
    op.drop_index("idx_leads_enrichment_sources", table_name="leads")
    op.drop_column("leads", "contact_category")
    op.drop_column("leads", "enrichment_sources")

    op.drop_index("idx_physician_enrichment_sources", table_name="physician")
    op.drop_column("physician", "enrichment_last_attempted_at")
    op.drop_column("physician", "enrichment_sources")
    op.drop_column("physician", "email_enriched_at")
    op.drop_column("physician", "personal_email_confidence")
    op.drop_column("physician", "personal_email")
