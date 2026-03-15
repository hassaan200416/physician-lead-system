/**
 * LeadRow.tsx
 * -----------
 * Single animated table row for one lead.
 *
 * Animates in with a staggered fade+slide using Framer Motion.
 * The delay is index * 30ms so rows cascade in one by one.
 *
 * Clicking the row (any cell except Review button) opens the
 * LeadDrawer. The Review button uses e.stopPropagation() to
 * prevent the drawer from opening at the same time.
 *
 * The review button changes appearance based on review state:
 *   - No review:  grey "Review" text with message icon
 *   - Has review: shows rating (e.g. "8/10") in matching color
 */

import { motion } from "framer-motion";
import { Mail, MapPin, Building2, Star, MessageSquare } from "lucide-react";
import { Badge } from "../ui/Badge";
import { ScoreRing } from "../ui/ScoreRing";
import { getTierColor, getConfidenceColor } from "../../lib/utils";
import type { Lead, LeadReview } from "../../types";

interface LeadRowProps {
  /** Full lead data for this row. */
  lead: Lead;
  /** Row index used to calculate staggered animation delay. */
  index: number;
  /** Existing review for this lead, if any. Undefined = not yet reviewed. */
  review?: LeadReview;
  /** Called when the row is clicked — opens LeadDrawer. */
  onClick: () => void;
  /** Called when the Review button is clicked — opens ReviewModal. */
  onReview: () => void;
}

/**
 * Color scale for rating values 1-10.
 * Red (poor) → orange → yellow → green → teal (excellent).
 */
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

      {/* Specialty — visible from lg */}
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

      {/* Organization — visible from xl */}
      <td
        className="px-3 py-3 hidden xl:table-cell max-w-[160px]"
        onClick={onClick}
      >
        <div className="flex items-center gap-1.5 text-sm text-slate-400">
          <Building2 size={12} className="text-slate-600 shrink-0" />
          <span className="truncate">{lead.organization_name}</span>
        </div>
      </td>

      {/* Email — visible from lg, truncated with max-w */}
      <td
        className="px-3 py-3 hidden lg:table-cell max-w-[200px]"
        onClick={onClick}
      >
        <div className="flex items-center gap-1.5">
          <Mail size={12} className="text-teal-500 shrink-0" />
          <span className="text-teal-300 font-mono text-xs truncate block">
            {lead.email}
          </span>
        </div>
      </td>

      {/* Location — visible from 2xl only */}
      <td
        className="px-3 py-3 hidden 2xl:table-cell whitespace-nowrap"
        onClick={onClick}
      >
        <div className="flex items-center gap-1 text-xs text-slate-500">
          <MapPin size={11} className="shrink-0" />
          {lead.city}, {lead.state}
        </div>
      </td>

      {/* Tier + Confidence badges */}
      <td className="px-3 py-3 w-32" onClick={onClick}>
        <div className="flex items-center gap-1 flex-wrap">
          <Badge className={getTierColor(lead.lead_tier)}>
            {lead.lead_tier}
          </Badge>
          <Badge className={getConfidenceColor(lead.email_confidence_level)}>
            {lead.email_confidence_level}
          </Badge>
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
