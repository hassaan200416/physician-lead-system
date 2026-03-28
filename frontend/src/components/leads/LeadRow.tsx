/**
 * LeadRow.tsx
 * -----------
 * Single animated table row for one lead.
 *
 * Changes from original:
 *   - Organization column → Phone column (org visible in drawer)
 *   - Email display uses personal_email first, falls back to email
 *   - Status badge shows contact_category (A/B) instead of lead_tier
 *   - Confidence badge uses personal_email_confidence first
 *   - Phone shown with phone icon, formatted for readability
 */

import { motion } from "framer-motion";
import { Mail, MapPin, Phone, Star, MessageSquare } from "lucide-react";
import { Badge } from "../ui/Badge";
import { ScoreRing } from "../ui/ScoreRing";
import { getCategoryColor, getConfidenceColor } from "../../lib/utils";
import type { Lead, LeadReview } from "../../types";

interface LeadRowProps {
  lead: Lead;
  index: number;
  review?: LeadReview;
  onClick: () => void;
  onReview: () => void;
}

const RATING_COLORS: Record<number, string> = {
  1: "#ef4444",
  2: "#f97316",
  3: "#f97316",
  4: "#eab308",
  5: "#eab308",
  6: "#84cc16",
  7: "#22c55e",
  8: "#10b981",
  9: "#14b8a6",
  10: "#2dd4bf",
};

export function LeadRow({
  lead,
  index,
  review,
  onClick,
  onReview,
}: LeadRowProps) {
  // Use personal_email (new canonical) first, fall back to legacy email
  const displayEmail = lead.personal_email || lead.email;
  const displayConfidence =
    lead.personal_email_confidence || lead.email_confidence_level;

  return (
    <motion.tr
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: index * 0.03 }}
      className="border-b border-white/5 hover:bg-white/3 cursor-pointer transition-colors group"
    >
      {/* Score */}
      <td className="px-3 py-3 w-14" onClick={onClick}>
        <ScoreRing score={lead.lead_score} />
      </td>

      {/* Physician name + NPI */}
      <td className="px-3 py-3 max-w-[160px]" onClick={onClick}>
        <div className="font-medium text-white text-sm group-hover:text-teal-300 transition-colors truncate">
          {lead.full_name}
        </div>
        <div className="text-xs text-slate-500 mt-0.5 font-mono truncate">
          {lead.npi}
        </div>
      </td>

      {/* Specialty */}
      <td
        className="px-3 py-3 hidden lg:table-cell max-w-[180px]"
        onClick={onClick}
      >
        <div className="text-sm text-slate-300 truncate">
          {lead.specialty_category}
        </div>
        <div className="text-xs text-slate-500 mt-0.5 truncate">
          {lead.specialty}
        </div>
      </td>

      {/* Phone — replaces Organization column */}
      <td
        className="px-3 py-3 hidden xl:table-cell max-w-[160px]"
        onClick={onClick}
      >
        {lead.mobile_phone ? (
          <div className="flex items-center gap-1.5">
            <Phone size={12} className="text-emerald-500 shrink-0" />
            <span className="text-emerald-300 font-mono text-xs truncate">
              {lead.mobile_phone}
            </span>
          </div>
        ) : (
          <div className="flex items-center gap-1.5">
            <Phone size={12} className="text-slate-700 shrink-0" />
            <span className="text-slate-600 text-xs">No phone</span>
          </div>
        )}
      </td>

      {/* Email */}
      <td
        className="px-3 py-3 hidden lg:table-cell max-w-[200px]"
        onClick={onClick}
      >
        {displayEmail ? (
          <div className="flex items-center gap-1.5">
            <Mail size={12} className="text-teal-500 shrink-0" />
            <span className="text-teal-300 font-mono text-xs truncate block">
              {displayEmail}
            </span>
          </div>
        ) : (
          <div className="flex items-center gap-1.5">
            <Mail size={12} className="text-slate-700 shrink-0" />
            <span className="text-slate-600 text-xs">No email</span>
          </div>
        )}
      </td>

      {/* Location */}
      <td
        className="px-3 py-3 hidden 2xl:table-cell whitespace-nowrap"
        onClick={onClick}
      >
        <div className="flex items-center gap-1 text-xs text-slate-500">
          <MapPin size={11} className="shrink-0" />
          {lead.city}, {lead.state}
        </div>
      </td>

      {/* Status — contact_category (A/B) + confidence */}
      <td className="px-3 py-3 w-32" onClick={onClick}>
        <div className="flex items-center gap-1 flex-wrap">
          {lead.contact_category && (
            <Badge className={getCategoryColor(lead.contact_category)}>
              {lead.contact_category}
            </Badge>
          )}
          {displayConfidence && (
            <Badge className={getConfidenceColor(displayConfidence)}>
              {displayConfidence}
            </Badge>
          )}
        </div>
      </td>

      {/* Review button */}
      <td className="px-3 py-3 w-24">
        <button
          onClick={(e) => {
            e.stopPropagation();
            onReview();
          }}
          className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs
                      border transition-all duration-150 whitespace-nowrap
                      ${
                        review
                          ? "border-transparent"
                          : "text-slate-500 border-white/8 hover:border-teal-500/40 hover:text-teal-400"
                      }`}
          style={
            review
              ? {
                  background: `${RATING_COLORS[review.rating]}15`,
                  borderColor: `${RATING_COLORS[review.rating]}35`,
                  color: RATING_COLORS[review.rating],
                }
              : {}
          }
        >
          {review ? (
            <>
              <Star size={11} fill="currentColor" />
              {review.rating}/10
            </>
          ) : (
            <>
              <MessageSquare size={11} />
              Review
            </>
          )}
        </button>
      </td>
    </motion.tr>
  );
}
