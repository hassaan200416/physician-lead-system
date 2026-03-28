// types/index.ts
// Central type definitions for the physician lead system.
//
// Lead type mirrors the leads table in Supabase exactly.
// All new enrichment columns (personal_email, mobile_phone,
// contact_category, enrichment_sources) are included here.
//
// contact_category is the authoritative lead tier for display:
//   A = phone + any email (ready for outreach)
//   B = email only, no phone
//
// lead_tier is the score-based tier (A/B/C/Archive) — kept for
// internal scoring logic but contact_category drives UI display.

export interface Lead {
  npi: string
  first_name: string
  last_name: string
  full_name: string
  credential: string
  specialty: string
  specialty_category: string
  organization_name: string
  practice_domain: string

  // ── Email fields ────────────────────────────────────────────
  // personal_email = canonical enriched email (new sources)
  // email          = legacy Hunter v1 email (read-only)
  // Use personal_email first, fall back to email for display
  personal_email: string | null
  personal_email_confidence: 'HIGH' | 'MEDIUM' | 'LOW' | null
  email: string | null                      // legacy — read-only
  email_confidence_score: number | null
  email_confidence_level: 'HIGH' | 'MEDIUM' | 'LOW' | null
  email_verification_status: string | null
  email_source: string | null
  practice_email: string | null

  // ── Phone fields ─────────────────────────────────────────────
  mobile_phone: string | null
  phone_confidence: 'HIGH' | 'MEDIUM' | 'LOW' | null

  // ── Contact category (contact-based tier) ────────────────────
  // A = phone + any email  →  Category A lead
  // B = any email, no phone → Category B lead
  // This is the primary display tier in the UI
  contact_category: 'A' | 'B' | null

  // ── Enrichment tracking ──────────────────────────────────────
  enrichment_sources: string[]              // e.g. ["hunter.io", "fullenrich"]

  // ── Location ─────────────────────────────────────────────────
  address_line_1: string | null
  city: string | null
  state: string | null
  zip: string | null

  // ── Scoring ──────────────────────────────────────────────────
  lead_score: number
  lead_tier: 'A' | 'B' | 'C' | 'Archive'   // score-based tier

  // ── Professional ─────────────────────────────────────────────
  years_of_experience: number | null
  experience_bucket: string | null
  license_count: number | null
  multi_state_flag: boolean

  // ── Audit ────────────────────────────────────────────────────
  created_at: string
  updated_at: string
}

export interface LeadStats {
  total: number
  tier_a: number
  tier_b: number
  tier_c: number
  high_confidence: number
  avg_score: number
  // contact-category counts
  category_a: number
  category_b: number
}

// contact_category filter — A/B only (no C, no trash)
export type CategoryFilter = 'ALL' | 'A' | 'B'

// kept for backward compat but CategoryFilter is preferred in UI
export type TierFilter = 'ALL' | 'A' | 'B' | 'C'
export type ConfidenceFilter = 'ALL' | 'HIGH' | 'MEDIUM'

export interface LeadReview {
  npi: string
  rating: number
  review_text: string
  status: string
  created_at: string
  updated_at: string
}