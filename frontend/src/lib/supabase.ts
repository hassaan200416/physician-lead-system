/**
 * lib/supabase.ts
 * ---------------
 * Initialises the Supabase client used by all hooks in this app.
 * The client uses the anon key, which is intentionally public — row-level
 * security on the Supabase side controls what this key can access.
 *
 * Required environment variables (frontend/.env.local):
 *   VITE_SUPABASE_URL       — Supabase project URL
 *   VITE_SUPABASE_ANON_KEY  — Supabase anon (public) key
 *
 * Throws at module load time if either variable is missing so the missing
 * config is caught immediately in dev rather than as a silent query failure.
 */
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY

if (!supabaseUrl || !supabaseAnonKey) {
  throw new Error('Missing Supabase environment variables')
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey)