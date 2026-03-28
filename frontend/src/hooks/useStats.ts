/**
 * useStats.ts
 * -----------
 * Fetches aggregate pipeline statistics from the `leads` table.
 *
 * Pulls the minimal column set needed for the dashboard (tier, category,
 * confidence, score) in a single query and derives all counts client-side.
 * Cached for 60 seconds — stats don't need to be real-time.
 *
 * Returns a LeadStats object:
 *   total / tier_a / tier_b / tier_c  — score-based tier counts
 *   high_confidence                   — leads with HIGH email confidence
 *   avg_score                         — mean lead score across all leads
 *   category_a / category_b           — contact-category counts (A/B)
 */
import { useQuery } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { LeadStats } from '../types'

export function useStats() {
  return useQuery({
    queryKey: ['lead-stats'],
    queryFn: async (): Promise<LeadStats> => {
      const { data, error } = await supabase
        .from('leads')
        .select('lead_tier, contact_category, email_confidence_level, personal_email_confidence, lead_score')

      if (error) throw error

      const rows = data ?? []
      const scores = rows.map(r => r.lead_score).filter(Boolean)

      return {
        total: rows.length,
        tier_a: rows.filter(r => r.lead_tier === 'A').length,
        tier_b: rows.filter(r => r.lead_tier === 'B').length,
        tier_c: rows.filter(r => r.lead_tier === 'C').length,
        high_confidence: rows.filter(r =>
          r.personal_email_confidence === 'HIGH' ||
          r.email_confidence_level === 'HIGH'
        ).length,
        avg_score: scores.length
          ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length)
          : 0,
        // contact-category counts
        category_a: rows.filter(r => r.contact_category === 'A').length,
        category_b: rows.filter(r => r.contact_category === 'B').length,
      }
    },
    staleTime: 60_000,
  })
}