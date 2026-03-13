import { motion } from "framer-motion";
import { Mail, MapPin, Building2, Star, MessageSquare } from "lucide-react";
import { Badge } from "../ui/Badge";
import { ScoreRing } from "../ui/ScoreRing";
import { getTierColor, getConfidenceColor } from "../../lib/utils";
import type { Lead, LeadReview } from "../../types";

interface LeadRowProps {
  lead: Lead;
  index: number;
  review?: LeadReview;
  onClick: () => void;
  onReview: () => void;
}

export function LeadRow({
  lead,
  index,
  review,
  onClick,
  onReview,
}: LeadRowProps) {
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

  return (
    <motion.tr
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: index * 0.03 }}
      className="border-b border-white/5 hover:bg-white/3 cursor-pointer transition-colors group"
    >
      {/* Score */}
      <td className="px-4 py-3 w-16" onClick={onClick}>
        <ScoreRing score={lead.lead_score} />
      </td>

      {/* Name */}
      <td className="px-4 py-3" onClick={onClick}>
        <div className="font-medium text-white text-sm group-hover:text-teal-300 transition-colors">
          {lead.full_name}
        </div>
        <div className="text-xs text-slate-500 mt-0.5 font-mono">
          {lead.npi}
        </div>
      </td>

      {/* Specialty */}
      <td className="px-4 py-3 hidden md:table-cell" onClick={onClick}>
        <div className="text-sm text-slate-300">{lead.specialty_category}</div>
        <div className="text-xs text-slate-500 mt-0.5 truncate max-w-[200px]">
          {lead.specialty}
        </div>
      </td>

      {/* Organization */}
      <td className="px-4 py-3 hidden lg:table-cell" onClick={onClick}>
        <div className="flex items-center gap-1.5 text-sm text-slate-400">
          <Building2 size={12} className="text-slate-600 shrink-0" />
          <span className="truncate max-w-[180px]">
            {lead.organization_name}
          </span>
        </div>
      </td>

      {/* Email */}
      <td className="px-4 py-3 hidden lg:table-cell" onClick={onClick}>
        <div className="flex items-center gap-1.5">
          <Mail size={12} className="text-teal-500 shrink-0" />
          <span className="text-teal-300 font-mono text-xs">{lead.email}</span>
        </div>
      </td>

      {/* Location */}
      <td className="px-4 py-3 hidden xl:table-cell" onClick={onClick}>
        <div className="flex items-center gap-1 text-xs text-slate-500">
          <MapPin size={11} />
          {lead.city}, {lead.state}
        </div>
      </td>

      {/* Status badges */}
      <td className="px-4 py-3" onClick={onClick}>
        <div className="flex items-center gap-1.5">
          <Badge className={getTierColor(lead.lead_tier)}>
            {lead.lead_tier}
          </Badge>
          <Badge className={getConfidenceColor(lead.email_confidence_level)}>
            {lead.email_confidence_level}
          </Badge>
        </div>
      </td>

      {/* Review button — does NOT trigger row click */}
      <td className="px-4 py-3 w-24">
        <button
          onClick={(e) => {
            e.stopPropagation();
            onReview();
          }}
          className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs
                      border transition-all duration-150
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
