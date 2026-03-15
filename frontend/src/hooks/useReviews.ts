/**
 * useReviews.ts
 * -------------
 * Data-fetching and mutation hooks for the lead review system.
 * Reviews are stored in the `lead_reviews` Supabase table,
 * one review per NPI (upsert pattern — create or update).
 *
 * Three hooks:
 *   useReview      — fetch single review by NPI
 *   useUpsertReview — create or update a review
 *   useAllReviews  — fetch all reviews (used to show rating badges in table)
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { supabase } from '../lib/supabase'
import type { LeadReview } from '../types'

/**
 * Fetches the review for a single lead by NPI.
 *
 * Returns null if no review exists yet — this is how the
 * ReviewModal distinguishes between "create" and "edit" mode.
 *
 * Query is disabled when NPI is empty to prevent unnecessary
 * requests while the ReviewModal is closed.
 *
 * @param npi - 10-digit National Provider Identifier.
 * @returns React Query result with `LeadReview | null`.
 */
export function useReview(npi: string) {
  return useQuery({
    queryKey: ['review', npi],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('lead_reviews')
        .select('*')
        .eq('npi', npi)
        .maybeSingle() // Returns null instead of error when no row found
      if (error) throw error
      return data as LeadReview | null
    },
    enabled: !!npi, // Skip query entirely when npi is empty string
  })
}

/**
 * Mutation hook for creating or updating a lead review.
 *
 * Uses Supabase upsert so the same function handles both
 * first-time submission and edits — no separate create/update
 * endpoints needed.
 *
 * On success, invalidates two query keys:
 *   - ['review', npi]  — refreshes the single review in ReviewModal
 *   - ['reviews-all']  — refreshes the rating badges in LeadTable
 *
 * @returns React Query mutation with `mutate(review)` function.
 *
 * @example
 * const { mutate, isPending } = useUpsertReview()
 * mutate({ npi: '1234567890', rating: 8, status: 'interested', review_text: 'Good lead' })
 */
export function useUpsertReview() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (review: Omit<LeadReview, 'created_at' | 'updated_at'>) => {
      const { data, error } = await supabase
        .from('lead_reviews')
        .upsert({
          ...review,
          updated_at: new Date().toISOString(), // Always stamp updated_at on every write
        })
        .select()
        .single()
      if (error) throw error
      return data
    },
    onSuccess: (_, vars) => {
      // Invalidate both caches so UI reflects the change immediately
      queryClient.invalidateQueries({ queryKey: ['review', vars.npi] })
      queryClient.invalidateQueries({ queryKey: ['reviews-all'] })
    },
  })
}

/**
 * Fetches all reviews across all leads.
 *
 * Used by LeadTable to build a reviewMap (npi → review) so each
 * LeadRow can show its rating badge without making individual
 * per-row queries.
 *
 * @returns React Query result with `LeadReview[]`.
 */
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