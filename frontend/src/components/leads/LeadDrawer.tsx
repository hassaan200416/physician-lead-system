import { motion, AnimatePresence } from "framer-motion";
import {
  X,
  Mail,
  MapPin,
  Building2,
  Award,
  Shield,
  Clock,
  type LucideIcon,
} from "lucide-react";
import { Badge } from "../ui/Badge";
import { ScoreRing } from "../ui/ScoreRing";
import { getTierColor, getConfidenceColor } from "../../lib/utils";
import type { Lead } from "../../types";

interface LeadDrawerProps {
  lead: Lead | null;
  onClose: () => void;
}

function InfoRow({
  icon: Icon,
  label,
  value,
}: {
  icon: LucideIcon;
  label: string;
  value?: string | number | null;
}) {
  if (!value) return null;
  return (
    <div className="flex items-start gap-3 py-2.5 border-b border-white/5 last:border-0">
      <Icon size={14} className="text-slate-500 mt-0.5 shrink-0" />
      <div>
        <div className="text-xs text-slate-500 mb-0.5">{label}</div>
        <div className="text-sm text-slate-200">{value}</div>
      </div>
    </div>
  );
}

export function LeadDrawer({ lead, onClose }: LeadDrawerProps) {
  return (
    <AnimatePresence>
      {lead && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50"
          />

          {/* Drawer */}
          <motion.div
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "spring", damping: 30, stiffness: 300 }}
            className="fixed right-0 top-0 h-screen w-96 bg-navy-900 border-l border-white/10
                       z-50 overflow-y-auto"
          >
            {/* Header */}
            <div
              className="sticky top-0 bg-navy-900/95 backdrop-blur-sm border-b border-white/10
                            px-5 py-4 flex items-start justify-between"
            >
              <div>
                <h2 className="font-display font-semibold text-white">
                  {lead.full_name}
                </h2>
                <p className="text-xs text-slate-400 mt-0.5 font-mono">
                  {lead.npi}
                </p>
              </div>
              <button
                onClick={onClose}
                className="text-slate-500 hover:text-white transition-colors p-1"
              >
                <X size={18} />
              </button>
            </div>

            <div className="p-5 space-y-5">
              {/* Score + Tier */}
              <div className="glass rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-500 mb-1 uppercase tracking-wider">
                    Lead Score
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge className={getTierColor(lead.lead_tier)}>
                      Tier {lead.lead_tier}
                    </Badge>
                    <Badge
                      className={getConfidenceColor(
                        lead.email_confidence_level,
                      )}
                    >
                      {lead.email_confidence_level}
                    </Badge>
                  </div>
                </div>
                <ScoreRing score={lead.lead_score} size={56} />
              </div>

              {/* Contact */}
              <div>
                <h3 className="text-xs text-slate-500 uppercase tracking-wider mb-3">
                  Contact
                </h3>
                <div className="glass rounded-xl px-4 divide-y divide-white/5">
                  <InfoRow icon={Mail} label="Email" value={lead.email} />
                  <InfoRow
                    icon={Shield}
                    label="Verification"
                    value={lead.email_verification_status}
                  />
                  <InfoRow
                    icon={Award}
                    label="Confidence Score"
                    value={`${lead.email_confidence_score} / 100`}
                  />
                </div>
              </div>

              {/* Practice */}
              <div>
                <h3 className="text-xs text-slate-500 uppercase tracking-wider mb-3">
                  Practice
                </h3>
                <div className="glass rounded-xl px-4 divide-y divide-white/5">
                  <InfoRow
                    icon={Building2}
                    label="Organization"
                    value={lead.organization_name}
                  />
                  <InfoRow
                    icon={MapPin}
                    label="Address"
                    value={lead.address_line_1}
                  />
                  <InfoRow
                    icon={MapPin}
                    label="Location"
                    value={[lead.city, lead.state, lead.zip]
                      .filter(Boolean)
                      .join(", ")}
                  />
                </div>
              </div>

              {/* Professional */}
              <div>
                <h3 className="text-xs text-slate-500 uppercase tracking-wider mb-3">
                  Professional
                </h3>
                <div className="glass rounded-xl px-4 divide-y divide-white/5">
                  <InfoRow
                    icon={Award}
                    label="Specialty"
                    value={lead.specialty}
                  />
                  <InfoRow
                    icon={Award}
                    label="Category"
                    value={lead.specialty_category}
                  />
                  <InfoRow
                    icon={Clock}
                    label="Experience"
                    value={lead.experience_bucket}
                  />
                  <InfoRow
                    icon={Shield}
                    label="Licenses"
                    value={lead.license_count?.toString()}
                  />
                </div>
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
