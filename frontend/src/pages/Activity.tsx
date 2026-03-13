import { motion } from "framer-motion";
import { Mail, MapPin, Building2, Clock } from "lucide-react";
import { TopBar } from "../components/layout/TopBar";
import { useLeads } from "../hooks/useLeads";
import { Badge } from "../components/ui/Badge";
import { ScoreRing } from "../components/ui/ScoreRing";
import { getTierColor, getConfidenceColor } from "../lib/utils";

export function Activity() {
  const { data } = useLeads({ pageSize: 10 });
  const recentLeads = data?.leads ?? [];

  return (
    <div>
      <TopBar title="Recent Activity" />
      <div className="p-6 space-y-4">
        <h2 className="text-xs text-slate-500 uppercase tracking-widest">
          Latest Leads Added
        </h2>

        {recentLeads.length === 0 && (
          <p className="text-sm text-slate-500 py-8 text-center">
            No leads yet.
          </p>
        )}

        {recentLeads.map((lead, index) => (
          <motion.div
            key={lead.npi}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.05 }}
            className="glass glass-hover rounded-xl p-4 flex items-center gap-4"
          >
            <ScoreRing score={lead.lead_score} size={48} />

            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1">
                <span className="text-sm font-medium text-white">
                  {lead.full_name}
                </span>
                <Badge className={getTierColor(lead.lead_tier)}>
                  Tier {lead.lead_tier}
                </Badge>
                <Badge
                  className={getConfidenceColor(lead.email_confidence_level)}
                >
                  {lead.email_confidence_level}
                </Badge>
              </div>

              <div className="flex items-center gap-4 text-xs text-slate-500 flex-wrap">
                <span className="flex items-center gap-1">
                  <Building2 size={11} />
                  {lead.organization_name}
                </span>
                <span className="flex items-center gap-1 text-teal-400/80 font-mono">
                  <Mail size={11} />
                  {lead.email}
                </span>
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
        ))}
      </div>
    </div>
  );
}
