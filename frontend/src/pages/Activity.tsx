/**
 * Activity.tsx
 * ------------
 * Shows two activity feeds side by side:
 *   1. Recently added leads (ordered by created_at)
 *   2. Recently reviewed leads (ordered by review updated_at)
 */

import { motion } from "framer-motion";
import {
  Mail,
  MapPin,
  Building2,
  Clock,
  Star,
  MessageSquare,
} from "lucide-react";
import { TopBar } from "../components/layout/TopBar";
import { useLeads } from "../hooks/useLeads";
import { useAllReviews } from "../hooks/useReviews";
import { Badge } from "../components/ui/Badge";
import { ScoreRing } from "../components/ui/ScoreRing";
import { getCategoryColor, getConfidenceColor } from "../lib/utils";

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

const STATUS_LABELS: Record<string, { label: string; color: string }> = {
  contacted: { label: "Contacted", color: "#3b82f6" },
  interested: { label: "Interested", color: "#10b981" },
  not_interested: { label: "Not Interested", color: "#ef4444" },
  converted: { label: "Converted", color: "#8b5cf6" },
  no_response: { label: "No Response", color: "#f59e0b" },
};

export function Activity() {
  const { data } = useLeads({ pageSize: 10 });
  const { data: reviews = [] } = useAllReviews();
  const recentLeads = data?.leads ?? [];

  const sortedReviews = [...reviews].sort(
    (a, b) =>
      new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime(),
  );

  const leadMap = Object.fromEntries(recentLeads.map((l) => [l.npi, l]));

  return (
    <div>
      <TopBar title="Activity" />
      <div className="p-6 grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Recently Added Leads */}
        <div>
          <h2 className="text-xs text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
            <MessageSquare size={12} />
            Recently Added Leads
          </h2>
          <div className="space-y-2">
            {recentLeads.length === 0 && (
              <p className="text-sm text-slate-500 py-4">No leads yet.</p>
            )}
            {recentLeads.map((lead, i) => {
              // Use personal_email first, fall back to legacy email
              const displayEmail = lead.personal_email || lead.email;
              // Use personal_email_confidence first, fall back to legacy
              const displayConfidence =
                lead.personal_email_confidence || lead.email_confidence_level;

              return (
                <motion.div
                  key={lead.npi}
                  initial={{ opacity: 0, x: -12 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.05 }}
                  className="glass glass-hover rounded-xl p-4 flex items-center gap-4"
                >
                  <ScoreRing score={lead.lead_score} size={44} />

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                      <span className="text-sm font-medium text-white truncate">
                        {lead.full_name}
                      </span>
                      {/* Show contact_category (A/B) badge */}
                      {lead.contact_category && (
                        <Badge
                          className={getCategoryColor(lead.contact_category)}
                        >
                          Cat {lead.contact_category}
                        </Badge>
                      )}
                      {/* Show confidence badge if available */}
                      {displayConfidence && (
                        <Badge
                          className={getConfidenceColor(displayConfidence)}
                        >
                          {displayConfidence}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-3 text-xs text-slate-500 flex-wrap">
                      <span className="flex items-center gap-1">
                        <Building2 size={11} />
                        <span className="truncate max-w-[140px]">
                          {lead.organization_name}
                        </span>
                      </span>
                      {displayEmail && (
                        <span className="flex items-center gap-1 text-teal-400/80 font-mono">
                          <Mail size={11} />
                          <span className="truncate max-w-[140px]">
                            {displayEmail}
                          </span>
                        </span>
                      )}
                      {lead.city && (
                        <span className="flex items-center gap-1">
                          <MapPin size={11} />
                          {lead.city}, {lead.state}
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="text-xs text-slate-600 flex items-center gap-1 shrink-0">
                    <Clock size={11} />
                    {new Date(lead.created_at).toLocaleDateString()}
                  </div>
                </motion.div>
              );
            })}
          </div>
        </div>

        {/* Recently Reviewed Leads */}
        <div>
          <h2 className="text-xs text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
            <Star size={12} />
            Recently Reviewed
          </h2>
          <div className="space-y-2">
            {sortedReviews.length === 0 && (
              <p className="text-sm text-slate-500 py-4">No reviews yet.</p>
            )}
            {sortedReviews.map((review, i) => {
              const lead = leadMap[review.npi];
              const statusInfo = STATUS_LABELS[review.status] ?? {
                label: review.status,
                color: "#64748b",
              };

              return (
                <motion.div
                  key={review.npi}
                  initial={{ opacity: 0, x: 12 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.05 }}
                  className="glass glass-hover rounded-xl p-4 flex items-center gap-4"
                >
                  <div
                    className="w-11 h-11 rounded-xl flex flex-col items-center justify-center shrink-0 border"
                    style={{
                      background: `${RATING_COLORS[review.rating]}15`,
                      borderColor: `${RATING_COLORS[review.rating]}35`,
                    }}
                  >
                    <Star
                      size={12}
                      fill={RATING_COLORS[review.rating]}
                      color={RATING_COLORS[review.rating]}
                    />
                    <span
                      className="text-xs font-mono font-bold mt-0.5"
                      style={{ color: RATING_COLORS[review.rating] }}
                    >
                      {review.rating}/10
                    </span>
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                      <span className="text-sm font-medium text-white truncate">
                        {lead?.full_name ?? review.npi}
                      </span>
                      <span
                        className="text-xs px-2 py-0.5 rounded-md border font-medium"
                        style={{
                          color: statusInfo.color,
                          background: `${statusInfo.color}15`,
                          borderColor: `${statusInfo.color}35`,
                        }}
                      >
                        {statusInfo.label}
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-xs text-slate-500 flex-wrap">
                      {lead && (
                        <span className="flex items-center gap-1">
                          <Building2 size={11} />
                          <span className="truncate max-w-[140px]">
                            {lead.organization_name}
                          </span>
                        </span>
                      )}
                      {review.review_text && (
                        <span className="text-slate-500 italic truncate max-w-[160px]">
                          "{review.review_text}"
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="text-xs text-slate-600 flex items-center gap-1 shrink-0">
                    <Clock size={11} />
                    {new Date(review.updated_at).toLocaleDateString()}
                  </div>
                </motion.div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
