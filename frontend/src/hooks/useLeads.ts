/**
 * useLeads.ts
 * -----------
 * Data-fetching hook for the leads table.
 * Queries the Supabase `leads` table with optional filters,
 * full-text search, and pagination.
 *
 * Caching: Results are cached for 30 seconds via React Query.
 * Re-fetches automatically when any filter or page changes.
 */

import { useQuery } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { Lead, TierFilter, ConfidenceFilter } from '../types'

interface UseLeadsOptions {
  /** Lead tier filter. 'ALL' returns all tiers. */
  tier?: TierFilter
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
 * Always orders results by lead_score DESC so highest-value
 * leads appear first regardless of active filters.
 *
 * The query key includes all filter params so React Query
 * automatically re-fetches whenever any filter changes.
 *
 * @param options - Filter, search, and pagination options.
 * @returns React Query result with `{ leads: Lead[], total: number }`.
 *
 * @example
 * const { data, isLoading } = useLeads({ tier: 'A', state: 'TX', page: 1 })
 */
export function useLeads({
  tier = 'ALL',
  confidence = 'ALL',
  state = '',
  search = '',
  page = 1,
  pageSize = 50,
}: UseLeadsOptions = {}) {
  return useQuery({
    queryKey: ['leads', tier, confidence, state, search, page],
    queryFn: async () => {
      let query = supabase
        .from('leads')
        .select('*', { count: 'exact' })
        .order('lead_score', { ascending: false })
        .range((page - 1) * pageSize, page * pageSize - 1)

      // Apply filters — only added when not at default "ALL" state
      if (tier !== 'ALL') query = query.eq('lead_tier', tier)
      if (confidence !== 'ALL') query = query.eq('email_confidence_level', confidence)
      if (state) query = query.eq('state', state)

      // Full-text search across 4 fields using Supabase OR filter
      if (search) {
        query = query.or(
          `first_name.ilike.%${search}%,last_name.ilike.%${search}%,` +
          `organization_name.ilike.%${search}%,email.ilike.%${search}%`
        )
      }

      const { data, error, count } = await query
      if (error) throw error
      return { leads: data as Lead[], total: count ?? 0 }
    },
    staleTime: 30_000, // Cache for 30 seconds before background refetch
  })
}