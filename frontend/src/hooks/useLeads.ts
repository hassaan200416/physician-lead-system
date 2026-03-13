import { useQuery } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { Lead, TierFilter, ConfidenceFilter } from '../types'

interface UseLeadsOptions {
  tier?: TierFilter
  confidence?: ConfidenceFilter
  state?: string
  search?: string
  page?: number
  pageSize?: number
}

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

      if (tier !== 'ALL') query = query.eq('lead_tier', tier)
      if (confidence !== 'ALL') query = query.eq('email_confidence_level', confidence)
      if (state) query = query.eq('state', state)
      if (search) {
        query = query.or(
          `first_name.ilike.%${search}%,last_name.ilike.%${search}%,organization_name.ilike.%${search}%,email.ilike.%${search}%`
        )
      }

      const { data, error, count } = await query
      if (error) throw error
      return { leads: data as Lead[], total: count ?? 0 }
    },
    staleTime: 30_000,
  })
}