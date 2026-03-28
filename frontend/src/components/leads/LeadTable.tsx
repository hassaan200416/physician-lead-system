/**
 * LeadTable.tsx
 * -------------
 * Renders the full leads table with headers and rows.
 *
 * Column layout:
 *   Score | Physician | Specialty | Phone | Email | Location | Status | Action
 *
 * Phone column replaces Organization — organization is still
 * visible in the LeadDrawer when a row is clicked.
 *
 * Category badge (A/B) replaces old tier badge in Status column.
 */

import { LeadRow } from "./LeadRow";
import type { Lead, LeadReview } from "../../types";

interface LeadTableProps {
  leads: Lead[];
  reviews: LeadReview[];
  onSelect: (lead: Lead) => void;
  onReview: (lead: Lead) => void;
}

export function LeadTable({
  leads,
  reviews,
  onSelect,
  onReview,
}: LeadTableProps) {
  const reviewMap = Object.fromEntries(reviews.map((r) => [r.npi, r]));

  return (
    <div className="w-full overflow-hidden">
      <table className="w-full">
        <thead>
          <tr className="border-b border-white/10">
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider w-14">
              Score
            </th>
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
              Physician
            </th>
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider hidden lg:table-cell">
              Specialty
            </th>
            {/* Phone replaces Organization — org visible in drawer */}
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider hidden xl:table-cell">
              Phone
            </th>
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider hidden lg:table-cell">
              Email
            </th>
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider hidden 2xl:table-cell">
              Location
            </th>
            <th className="px-3 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider w-32">
              Status
            </th>
            <th className="px-3 py-3 w-24" />
          </tr>
        </thead>
        <tbody>
          {leads.map((lead, i) => (
            <LeadRow
              key={lead.npi}
              lead={lead}
              index={i}
              review={reviewMap[lead.npi]}
              onClick={() => onSelect(lead)}
              onReview={() => onReview(lead)}
            />
          ))}
        </tbody>
      </table>

      {leads.length === 0 && (
        <div className="text-center py-16 text-slate-500">
          <p className="text-sm">No leads match your filters</p>
        </div>
      )}
    </div>
  );
}
