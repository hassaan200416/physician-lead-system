/**
 * LeadTable.tsx
 * -------------
 * Renders the full leads table with header row and animated data rows.
 *
 * Builds a reviewMap from the reviews array so each LeadRow can
 * look up its review in O(1) without prop drilling or extra queries.
 *
 * Columns (responsive — some hidden on smaller screens):
 *   Score | Physician | Specialty | Organization | Email |
 *   Location | Status | Review
 */

import { LeadRow } from "./LeadRow";
import type { Lead, LeadReview } from "../../types";

interface LeadTableProps {
  /** Leads to display in the table, already filtered and paginated. */
  leads: Lead[];
  /** All reviews fetched from Supabase — used to show rating badges. */
  reviews: LeadReview[];
  /** Called when a row is clicked — opens the LeadDrawer. */
  onSelect: (lead: Lead) => void;
  /** Called when the Review button is clicked — opens ReviewModal. */
  onReview: (lead: Lead) => void;
}

export function LeadTable({
  leads,
  reviews,
  onSelect,
  onReview,
}: LeadTableProps) {
  /**
   * Build lookup map: npi → review
   * Allows O(1) review lookup per row instead of O(n) array scan.
   */
  const reviewMap = Object.fromEntries(reviews.map((r) => [r.npi, r]));

  return (
    <div className="w-full overflow-x-auto">
      <table className="w-full min-w-[640px]">
        <thead>
          <tr className="border-b border-white/10">
            {[
              "Score",
              "Physician",
              "Specialty",
              "Organization",
              "Email",
              "Location",
              "Status",
              "",
            ].map((h, i) => (
              <th
                key={i}
                className="px-4 py-3 text-left text-xs font-medium text-slate-500
                           uppercase tracking-wider"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {leads.map((lead, i) => (
            <LeadRow
              key={lead.npi}
              lead={lead}
              index={i}
              review={reviewMap[lead.npi]} // undefined if no review yet
              onClick={() => onSelect(lead)}
              onReview={() => onReview(lead)}
            />
          ))}
        </tbody>
      </table>

      {/* Empty state — shown when filters return no results */}
      {leads.length === 0 && (
        <div className="text-center py-16 text-slate-500">
          <p className="text-sm">No leads match your filters</p>
        </div>
      )}
    </div>
  );
}
