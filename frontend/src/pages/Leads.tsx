/**
 * Leads.tsx
 * ---------
 * Main leads page — filter bar, table, drawer, review modal.
 *
 * Filter state:
 *   category   — contact_category filter (A/B/ALL)
 *   confidence — email confidence filter (HIGH/MEDIUM/ALL)
 *   state      — US state code filter
 *   search     — full-text search across name, org, email
 *   page       — current pagination page (1-indexed)
 *
 * category replaces the old tier filter. It filters by
 * contact_category (A = phone+email, B = email only) rather
 * than the score-based lead_tier.
 */

import { useMemo, useState } from "react";
import { TopBar } from "../components/layout/TopBar";
import { LeadFilters } from "../components/leads/LeadFilters";
import { LeadTable } from "../components/leads/LeadTable";
import { LeadDrawer } from "../components/leads/LeadDrawer";
import { ReviewModal } from "../components/leads/ReviewModal";
import { useLeads } from "../hooks/useLeads";
import { useAllReviews } from "../hooks/useReviews";
import type { Lead, CategoryFilter, ConfidenceFilter } from "../types";

export function Leads() {
  const [category, setCategory] = useState<CategoryFilter>("ALL");
  const [confidence, setConfidence] = useState<ConfidenceFilter>("ALL");
  const [state, setState] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Lead | null>(null);
  const [reviewing, setReviewing] = useState<Lead | null>(null);
  const [page, setPage] = useState(1);

  const { data, isLoading } = useLeads({
    category,
    confidence,
    state,
    search,
    page,
  });
  const { data: reviews = [] } = useAllReviews();

  // Derive unique state codes from current leads for the state dropdown
  const states = useMemo(() => {
    const all = data?.leads?.map((l) => l.state).filter(Boolean) as string[];
    return [...new Set(all)].sort();
  }, [data?.leads]);

  return (
    <div>
      <TopBar
        title="Leads"
        search={search}
        onSearch={(v) => {
          setSearch(v);
          setPage(1);
        }}
      />

      <LeadFilters
        category={category}
        confidence={confidence}
        state={state}
        states={states}
        onCategory={(v) => {
          setCategory(v);
          setPage(1);
        }}
        onConfidence={(v) => {
          setConfidence(v);
          setPage(1);
        }}
        onState={(v) => {
          setState(v);
          setPage(1);
        }}
      />

      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <div className="w-6 h-6 border-2 border-teal-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <LeadTable
          leads={data?.leads ?? []}
          reviews={reviews}
          onSelect={setSelected}
          onReview={setReviewing}
        />
      )}

      {/* Pagination — only shown when total exceeds page size */}
      {data && data.total > 50 && (
        <div className="flex items-center justify-between px-6 py-3 border-t border-white/5">
          <span className="text-xs text-slate-500">
            {data.total} total leads
          </span>
          <div className="flex gap-2">
            <button
              disabled={page === 1}
              onClick={() => setPage((p) => p - 1)}
              className="px-3 py-1 rounded-md text-xs border border-white/10 text-slate-400
                         disabled:opacity-30 hover:border-teal-500/40 transition-colors"
            >
              Prev
            </button>
            <span className="px-3 py-1 text-xs text-slate-400">
              Page {page}
            </span>
            <button
              disabled={page * 50 >= data.total}
              onClick={() => setPage((p) => p + 1)}
              className="px-3 py-1 rounded-md text-xs border border-white/10 text-slate-400
                         disabled:opacity-30 hover:border-teal-500/40 transition-colors"
            >
              Next
            </button>
          </div>
        </div>
      )}

      <LeadDrawer lead={selected} onClose={() => setSelected(null)} />
      <ReviewModal
        key={reviewing?.npi ?? "closed"}
        lead={reviewing}
        onClose={() => setReviewing(null)}
      />
    </div>
  );
}
