import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { LeadReview } from '../types'

export function useReview(npi: string) {
  return useQuery({
    queryKey: ['review', npi],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('lead_reviews')
        .select('*')
        .eq('npi', npi)
        .maybeSingle()
      if (error) throw error
      return data as LeadReview | null
    },
    enabled: !!npi,
  })
}

export function useUpsertReview() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (review: Omit<LeadReview, 'created_at' | 'updated_at'>) => {
      const { data, error } = await supabase
        .from('lead_reviews')
        .upsert({ ...review, updated_at: new Date().toISOString() })
        .select()
        .single()
      if (error) throw error
      return data
    },
    onSuccess: (_, vars) => {
      queryClient.invalidateQueries({ queryKey: ['review', vars.npi] })
      queryClient.invalidateQueries({ queryKey: ['reviews-all'] })
    },
  })
}

export function useAllReviews() {
  return useQuery({
    queryKey: ['reviews-all'],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('lead_reviews')
        .select('*')
      if (error) throw error
      return (data ?? []) as LeadReview[]
    },
  })
}