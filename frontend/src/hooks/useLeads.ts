/**
 * useLeads.ts
 * -----------
 * Data-fetching hook for the leads table.
 * Queries the Supabase `leads` table with optional filters,
 * full-text search, and pagination.
 *
 * Filter logic:
 *   category filter → filters by contact_category (A/B) — contact-based
 *   confidence filter → filters by email_confidence_level
 *   state filter → filters by state code
 *   search → searches name, organization, personal_email, email
 *
 * Caching: Results are cached for 30 seconds via React Query.
 * Re-fetches automatically when any filter or page changes.
 */

import { useQuery } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { Lead, CategoryFilter, ConfidenceFilter } from '../types'

interface UseLeadsOptions {
  /** Contact category filter. 'ALL' returns all categories. */
  category?: CategoryFilter
  /** Email confidence filter. 'ALL' returns all confidence levels. */
  confidence?: ConfidenceFilter
  /** 2-letter state code filter. Empty string returns all states. */
  state?: string
  /** Full-text search across name, organization, and email fields. */
  search?: string
  /** Current page number (1-indexed). */
  page?: number
  /** Number of records per page. Default 50. */
  pageSize?: number
}

/**
 * Fetches a paginated, filtered list of leads from Supabase.
 *
 * Always orders by lead_score DESC so highest-value leads
 * appear first regardless of active filters.
 *
 * Only returns leads that have at least one contact signal
 * (personal_email OR email OR mobile_phone) — trash leads
 * are never inserted into the leads table so no extra filter needed.
 *
 * @param options - Filter, search, and pagination options.
 * @returns React Query result with `{ leads: Lead[], total: number }`.
 */
export function useLeads({
  category = 'ALL',
  confidence = 'ALL',
  state = '',
  search = '',
  page = 1,
  pageSize = 50,
}: UseLeadsOptions = {}) {
  return useQuery({
    queryKey: ['leads', category, confidence, state, search, page],
    queryFn: async () => {
      let query = supabase
        .from('leads')
        .select('*', { count: 'exact' })
        .order('lead_score', { ascending: false })
        .range((page - 1) * pageSize, page * pageSize - 1)

      // contact_category filter — A = phone+email, B = email only
      if (category !== 'ALL') {
        query = query.eq('contact_category', category)
      }

      // confidence filter on personal_email_confidence (preferred)
      // falls back to checking email_confidence_level for legacy records
      if (confidence !== 'ALL') {
        query = query.or(
          `personal_email_confidence.eq.${confidence},` +
          `email_confidence_level.eq.${confidence}`
        )
      }

      if (state) query = query.eq('state', state)

      // Search across name fields, org, and both email columns
      if (search) {
        query = query.or(
          `first_name.ilike.%${search}%,` +
          `last_name.ilike.%${search}%,` +
          `organization_name.ilike.%${search}%,` +
          `personal_email.ilike.%${search}%,` +
          `email.ilike.%${search}%`
        )
      }

      const { data, error, count } = await query
      if (error) throw error
      return { leads: data as Lead[], total: count ?? 0 }
    },
    staleTime: 30_000,
  })
}