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
    <div className="overflow-x-auto">
      <table className="w-full">
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
