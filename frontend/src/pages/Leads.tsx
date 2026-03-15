import { useMemo, useState } from "react";
import { TopBar } from "../components/layout/TopBar";
import { LeadFilters } from "../components/leads/LeadFilters";
import { LeadTable } from "../components/leads/LeadTable";
import { LeadDrawer } from "../components/leads/LeadDrawer";
import { ReviewModal } from "../components/leads/ReviewModal";
import { useLeads } from "../hooks/useLeads";
import { useAllReviews } from "../hooks/useReviews";
import type { Lead, TierFilter, ConfidenceFilter } from "../types";

export function Leads() {
  const [tier, setTier] = useState<TierFilter>("ALL");
  const [confidence, setConfidence] = useState<ConfidenceFilter>("ALL");
  const [state, setState] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Lead | null>(null);
  const [reviewing, setReviewing] = useState<Lead | null>(null);
  const [page, setPage] = useState(1);

  const { data, isLoading } = useLeads({
    tier,
    confidence,
    state,
    search,
    page,
  });
  const { data: reviews = [] } = useAllReviews();

  const states = useMemo(() => {
    const all = data?.leads?.map((l) => l.state).filter(Boolean) ?? [];
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
        tier={tier}
        confidence={confidence}
        state={state}
        states={states}
        onTier={(v) => {
          setTier(v);
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
