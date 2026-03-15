/**
 * ReviewModal.tsx
 * ---------------
 * Centered modal dialog for submitting or editing a lead review.
 *
 * Opens when the Review button in LeadRow is clicked.
 * Detects whether a review already exists (via useReview hook)
 * and switches between "Mark as Used" (create) and "Edit Review"
 * mode accordingly.
 *
 * Form fields:
 *   Rating     — 1-10 scale, color-coded red→teal, required
 *   Status     — contact outcome tag (contacted, interested, etc.)
 *   Notes      — free-text field, optional
 *
 * On submit: upserts to lead_reviews table via useUpsertReview.
 * Shows spinner during request, success state for 1.2s then closes.
 *
 * Animation: scales in from 95% with spring physics.
 * Clicking outside the modal (on backdrop) closes it.
 */

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { X, Star, Send, Loader2 } from "lucide-react";
import { useReview, useUpsertReview } from "../../hooks/useReviews";
import type { Lead } from "../../types";

interface ReviewModalProps {
  /** Lead being reviewed. Null when modal is closed. */
  lead: Lead | null;
  /** Called to close the modal. */
  onClose: () => void;
}

/** Contact outcome options shown as toggle buttons. */
const STATUS_OPTIONS = [
  {
    value: "contacted",
    label: "Contacted",
    color: "#3b82f6",
    bg: "#3b82f610",
    border: "#3b82f640",
  },
  {
    value: "interested",
    label: "Interested",
    color: "#10b981",
    bg: "#10b98110",
    border: "#10b98140",
  },
  {
    value: "not_interested",
    label: "Not Interested",
    color: "#ef4444",
    bg: "#ef444410",
    border: "#ef444440",
  },
  {
    value: "converted",
    label: "Converted",
    color: "#8b5cf6",
    bg: "#8b5cf610",
    border: "#8b5cf640",
  },
  {
    value: "no_response",
    label: "No Response",
    color: "#f59e0b",
    bg: "#f59e0b10",
    border: "#f59e0b40",
  },
];

/**
 * Color scale for rating buttons 1-10.
 * Colors progress from red (poor) through orange, yellow,
 * green, to teal (excellent). Ratings 2-3 and 4-5 share
 * colors as natural groupings.
 */
const RATING_COLORS: Record<number, string> = {
  1: "#ef4444", // Red — Poor
  2: "#f97316", // Orange
  3: "#f97316",
  4: "#eab308", // Yellow
  5: "#eab308",
  6: "#84cc16", // Lime
  7: "#22c55e", // Green
  8: "#10b981", // Emerald
  9: "#14b8a6", // Teal
  10: "#2dd4bf", // Bright teal — Excellent
};

export function ReviewModal({ lead, onClose }: ReviewModalProps) {
  const { data: existing, isLoading } = useReview(lead?.npi ?? "");
  const { mutate: upsert, isPending } = useUpsertReview();

  const [rating, setRating] = useState(0);
  const [hovered, setHovered] = useState(0);
  const [status, setStatus] = useState("contacted");
  const [reviewText, setReview] = useState("");
  const [saved, setSaved] = useState(false);
  const [prefilled, setPrefilled] = useState(false);

  if (existing && !prefilled) {
    setRating(existing.rating);
    setStatus(existing.status);
    setReview(existing.review_text ?? "");
    setPrefilled(true);
  }

  // Use hovered value for preview, fall back to selected rating
  const activeRating = hovered || rating;
  const accentColor = activeRating ? RATING_COLORS[activeRating] : "#2dd4bf";
  const isEdit = !!existing; // True = editing, False = first submission

  if (!lead) return null;
  if (isLoading)
    return (
      <AnimatePresence>
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50
                     flex items-center justify-center"
        >
          <div
            className="w-6 h-6 border-2 border-teal-500 border-t-transparent
                        rounded-full animate-spin"
          />
        </motion.div>
      </AnimatePresence>
    );

  function handleSubmit() {
    if (!lead || rating === 0) return;
    upsert(
      { npi: lead.npi, rating, review_text: reviewText, status },
      {
        onSuccess: () => {
          setSaved(true);
          // Show success state briefly before closing
          setTimeout(() => {
            setSaved(false);
            onClose();
          }, 1200);
        },
      },
    );
  }
  return (
    <AnimatePresence>
      {/* Backdrop — clicking closes modal */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50
                   flex items-center justify-center p-4"
      >
        {/* Modal panel — stopPropagation prevents backdrop click from firing */}
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ type: "spring", damping: 25, stiffness: 300 }}
          onClick={(e) => e.stopPropagation()}
          className="w-full max-w-md bg-navy-900 border border-white/10 rounded-2xl
                     shadow-2xl shadow-black/60 overflow-hidden"
        >
          {/* Header — background tints to match active rating color */}
          <div
            className="px-6 py-5 border-b border-white/10 flex items-start justify-between"
            style={{ background: `${accentColor}08` }}
          >
            <div>
              <h2 className="font-display font-semibold text-white">
                {isEdit ? "Edit Review" : "Mark as Used"}
              </h2>
              <p className="text-xs text-slate-400 mt-0.5">{lead.full_name}</p>
            </div>
            <button
              onClick={onClose}
              className="text-slate-500 hover:text-white transition-colors p-1 -mt-1"
            >
              <X size={18} />
            </button>
          </div>

          <div className="p-6 space-y-5">
            {/* Rating selector */}
            <div>
              <label className="text-xs text-slate-500 uppercase tracking-wider block mb-3">
                Rating
              </label>
              <div className="flex gap-1.5">
                {Array.from({ length: 10 }, (_, i) => i + 1).map((n) => (
                  <button
                    key={n}
                    onMouseEnter={() => setHovered(n)}
                    onMouseLeave={() => setHovered(0)}
                    onClick={() => setRating(n)}
                    className="flex-1 h-8 rounded-md text-xs font-mono font-semibold
                               border transition-all duration-100"
                    style={{
                      background:
                        n <= activeRating
                          ? `${RATING_COLORS[n]}20`
                          : "rgba(255,255,255,0.03)",
                      borderColor:
                        n <= activeRating
                          ? `${RATING_COLORS[n]}50`
                          : "rgba(255,255,255,0.08)",
                      color: n <= activeRating ? RATING_COLORS[n] : "#475569",
                    }}
                  >
                    {n}
                  </button>
                ))}
              </div>

              {/* Strength bar — fixed height container stops layout shift/vibration */}
              <div className="mt-3 space-y-1.5">
                <div className="h-1.5 w-full bg-white/5 rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-300"
                    style={{
                      width: activeRating ? `${activeRating * 10}%` : "0%",
                      background: activeRating
                        ? RATING_COLORS[activeRating]
                        : "transparent",
                    }}
                  />
                </div>
                {/* Fixed height so text appearing/disappearing doesn't shift layout */}
                <div className="h-4">
                  {activeRating > 0 && (
                    <p
                      className="text-xs transition-colors duration-200"
                      style={{ color: RATING_COLORS[activeRating] }}
                    >
                      {activeRating <= 2
                        ? "Poor lead"
                        : activeRating <= 4
                          ? "Below average"
                          : activeRating <= 6
                            ? "Average lead"
                            : activeRating <= 8
                              ? "Good lead"
                              : "Excellent lead"}
                    </p>
                  )}
                </div>
              </div>
            </div>

            {/* Status toggle buttons — each with unique color */}
            <div>
              <label className="text-xs text-slate-500 uppercase tracking-wider block mb-2">
                Status
              </label>
              <div className="flex flex-wrap gap-2">
                {STATUS_OPTIONS.map((opt) => (
                  <button
                    key={opt.value}
                    onClick={() => setStatus(opt.value)}
                    className="px-3 py-1.5 rounded-lg text-xs border transition-all duration-150"
                    style={
                      status === opt.value
                        ? {
                            color: opt.color,
                            background: opt.bg,
                            borderColor: opt.border,
                          }
                        : {
                            color: "#64748b",
                            background: "transparent",
                            borderColor: "rgba(255,255,255,0.08)",
                          }
                    }
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Optional notes textarea */}
            <div>
              <label className="text-xs text-slate-500 uppercase tracking-wider block mb-2">
                Notes{" "}
                <span className="normal-case text-slate-600">(optional)</span>
              </label>
              <textarea
                value={reviewText}
                onChange={(e) => setReview(e.target.value)}
                placeholder="Add notes about this lead..."
                rows={3}
                className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3
                           text-sm text-slate-300 placeholder:text-slate-600
                           focus:outline-none focus:border-teal-500/40 focus:bg-white/7
                           resize-none transition-all"
              />
            </div>

            {/* Submit button — disabled if no rating selected or request in-flight */}
            <button
              onClick={handleSubmit}
              disabled={rating === 0 || isPending || saved}
              className="w-full py-3 rounded-xl text-sm font-medium transition-all duration-200
                         disabled:opacity-40 disabled:cursor-not-allowed
                         flex items-center justify-center gap-2"
              style={{
                background: saved ? "#10b98120" : `${accentColor}20`,
                border: `1px solid ${saved ? "#10b981" : accentColor}40`,
                color: saved ? "#10b981" : accentColor,
              }}
            >
              {saved ? (
                <>
                  <Star size={15} fill="currentColor" /> Saved!
                </>
              ) : isPending ? (
                <Loader2 size={15} className="animate-spin" />
              ) : (
                <>
                  <Send size={14} />
                  {isEdit ? "Update Review" : "Submit Review"}
                </>
              )}
            </button>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>
  );
}
