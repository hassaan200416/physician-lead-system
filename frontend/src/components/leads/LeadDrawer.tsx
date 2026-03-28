/**
 * LeadDrawer.tsx
 * --------------
 * Slide-in side panel showing full detail for a selected lead.
 *
 * Changes from original:
 *   - Contact section: shows mobile_phone + personal_email (canonical)
 *     falls back to legacy email for older Hunter records
 *   - Score card: shows contact_category (A/B) badge alongside lead_tier
 *   - Enrichment section: shows which sources found data for this lead
 *   - Organization kept here (removed from table but useful in detail)
 *   - Phone displayed with verification confidence if available
 */

import { motion, AnimatePresence } from "framer-motion";
import {
  X,
  Mail,
  MapPin,
  Building2,
  Award,
  Shield,
  Clock,
  Phone,
  Database,
  type LucideIcon,
} from "lucide-react";
import { Badge } from "../ui/Badge";
import { ScoreRing } from "../ui/ScoreRing";
import {
  getTierColor,
  getCategoryColor,
  getConfidenceColor,
} from "../../lib/utils";
import type { Lead } from "../../types";

interface LeadDrawerProps {
  lead: Lead | null;
  onClose: () => void;
}

function InfoRow({
  icon: Icon,
  label,
  value,
  mono = false,
}: {
  icon: LucideIcon;
  label: string;
  value?: string | number | null;
  mono?: boolean;
}) {
  if (!value && value !== 0) return null;
  return (
    <div className="flex items-start gap-3 py-2.5 border-b border-white/5 last:border-0">
      <Icon size={14} className="text-slate-500 mt-0.5 shrink-0" />
      <div className="min-w-0">
        <div className="text-xs text-slate-500 mb-0.5">{label}</div>
        <div
          className={`text-sm text-slate-200 break-all ${mono ? "font-mono" : ""}`}
        >
          {value}
        </div>
      </div>
    </div>
  );
}

/** Formats source names for display e.g. "hunter.io" → "Hunter.io" */
function formatSource(source: string): string {
  return source
    .split(".")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(".");
}

export function LeadDrawer({ lead, onClose }: LeadDrawerProps) {
  if (!lead) return null;

  // Canonical email — personal_email first, fall back to legacy email
  const displayEmail = lead.personal_email || lead.email;
  const displayEmailConfidence =
    lead.personal_email_confidence || lead.email_confidence_level;
  const emailSource = lead.personal_email
    ? "Enrichment"
    : lead.email
      ? "Hunter.io (legacy)"
      : null;

  const hasEnrichmentSources =
    Array.isArray(lead.enrichment_sources) &&
    lead.enrichment_sources.length > 0;

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

          {/* Drawer panel */}
          <motion.div
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "spring", damping: 30, stiffness: 300 }}
            className="fixed right-0 top-0 h-screen w-96 bg-navy-900 border-l border-white/10
                       z-50 overflow-y-auto isolate"
          >
            {/* Sticky header */}
            <div
              className="sticky top-0 z-10 bg-navy-900 border-b border-white/10
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
              {/* Score card */}
              <div className="glass rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-500 mb-2 uppercase tracking-wider">
                    Lead Score
                  </div>
                  <div className="flex items-center gap-2 flex-wrap">
                    {/* Contact category — primary display tier */}
                    {lead.contact_category && (
                      <Badge
                        className={getCategoryColor(lead.contact_category)}
                      >
                        Cat {lead.contact_category}
                      </Badge>
                    )}
                    {/* Score-based tier */}
                    <Badge className={getTierColor(lead.lead_tier)}>
                      Tier {lead.lead_tier}
                    </Badge>
                  </div>
                </div>
                <ScoreRing score={lead.lead_score} size={56} />
              </div>

              {/* Contact — phone + email */}
              <div>
                <h3 className="text-xs text-slate-500 uppercase tracking-wider mb-3">
                  Contact
                </h3>
                <div className="glass rounded-xl px-4 divide-y divide-white/5">
                  {/* Phone */}
                  {lead.mobile_phone ? (
                    <InfoRow
                      icon={Phone}
                      label={`Mobile Phone${lead.phone_confidence ? ` (${lead.phone_confidence})` : ""}`}
                      value={lead.mobile_phone}
                      mono
                    />
                  ) : (
                    <div className="flex items-start gap-3 py-2.5 border-b border-white/5">
                      <Phone
                        size={14}
                        className="text-slate-700 mt-0.5 shrink-0"
                      />
                      <div>
                        <div className="text-xs text-slate-500 mb-0.5">
                          Mobile Phone
                        </div>
                        <div className="text-sm text-slate-600">Not found</div>
                      </div>
                    </div>
                  )}

                  {/* Email */}
                  <InfoRow
                    icon={Mail}
                    label={`Email${displayEmailConfidence ? ` (${displayEmailConfidence})` : ""}${emailSource ? ` · ${emailSource}` : ""}`}
                    value={displayEmail}
                    mono
                  />

                  {/* Practice email if different from personal */}
                  {lead.practice_email &&
                    lead.practice_email !== displayEmail && (
                      <InfoRow
                        icon={Mail}
                        label="Practice Email (NPPES)"
                        value={lead.practice_email}
                        mono
                      />
                    )}

                  <InfoRow
                    icon={Shield}
                    label="Verification"
                    value={lead.email_verification_status}
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

              {/* Enrichment sources */}
              {hasEnrichmentSources && (
                <div>
                  <h3 className="text-xs text-slate-500 uppercase tracking-wider mb-3">
                    Data Sources
                  </h3>
                  <div className="glass rounded-xl px-4 py-3">
                    <div className="flex items-center gap-2 mb-2">
                      <Database size={12} className="text-slate-500" />
                      <span className="text-xs text-slate-500">
                        Contact data found by:
                      </span>
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {lead.enrichment_sources.map((source) => (
                        <span
                          key={source}
                          className="px-2 py-0.5 rounded-full text-xs
                                     bg-teal-500/10 text-teal-400 border border-teal-500/20"
                        >
                          {formatSource(source)}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
