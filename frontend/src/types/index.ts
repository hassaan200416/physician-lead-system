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
  email: string
  email_confidence_score: number
  email_confidence_level: 'HIGH' | 'MEDIUM' | 'LOW'
  email_verification_status: string
  email_source: string
  address_line_1: string
  city: string
  state: string
  zip: string
  lead_score: number
  lead_tier: 'A' | 'B' | 'C' | 'Archive'
  years_of_experience: number
  experience_bucket: string
  license_count: number
  multi_state_flag: boolean
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
}

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