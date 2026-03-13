import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { X, Star, Send, Loader2 } from "lucide-react";
import { useReview, useUpsertReview } from "../../hooks/useReviews";
import type { Lead } from "../../types";

interface ReviewModalProps {
  lead: Lead | null;
  onClose: () => void;
}

const STATUS_OPTIONS = [
  { value: "contacted", label: "Contacted" },
  { value: "interested", label: "Interested" },
  { value: "not_interested", label: "Not Interested" },
  { value: "converted", label: "Converted" },
  { value: "no_response", label: "No Response" },
];

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

export function ReviewModal({ lead, onClose }: ReviewModalProps) {
  const { data: existing } = useReview(lead?.npi ?? "");
  const { mutate: upsert, isPending } = useUpsertReview();

  const [rating, setRating] = useState(() => existing?.rating ?? 0);
  const [hovered, setHovered] = useState(0);
  const [status, setStatus] = useState(() => existing?.status ?? "contacted");
  const [reviewText, setReview] = useState(() => existing?.review_text ?? "");
  const [saved, setSaved] = useState(false);

  const activeRating = hovered || rating;
  const accentColor = activeRating ? RATING_COLORS[activeRating] : "#2dd4bf";
  const isEdit = !!existing;

  function handleSubmit() {
    if (!lead || rating === 0) return;
    upsert(
      { npi: lead.npi, rating, review_text: reviewText, status },
      {
        onSuccess: () => {
          setSaved(true);
          setTimeout(() => {
            setSaved(false);
            onClose();
          }, 1200);
        },
      },
    );
  }

  if (!lead) return null;

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        onClick={onClose}
        className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4"
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ type: "spring", damping: 25, stiffness: 300 }}
          onClick={(e) => e.stopPropagation()}
          className="w-full max-w-md bg-navy-900 border border-white/10 rounded-2xl
                     shadow-2xl shadow-black/60 overflow-hidden"
        >
          {/* Header */}
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
            {/* Rating */}
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
              {activeRating > 0 && (
                <p
                  className="text-xs mt-2 transition-all"
                  style={{ color: accentColor }}
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

            {/* Status */}
            <div>
              <label className="text-xs text-slate-500 uppercase tracking-wider block mb-2">
                Status
              </label>
              <div className="flex flex-wrap gap-2">
                {STATUS_OPTIONS.map((opt) => (
                  <button
                    key={opt.value}
                    onClick={() => setStatus(opt.value)}
                    className={`px-3 py-1.5 rounded-lg text-xs border transition-all duration-150
                                ${
                                  status === opt.value
                                    ? "text-teal-400 border-teal-500/40 bg-teal-500/10"
                                    : "text-slate-500 border-white/8 hover:border-white/15 hover:text-slate-300"
                                }`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Review text */}
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

            {/* Submit */}
            <button
              onClick={handleSubmit}
              disabled={rating === 0 || isPending || saved}
              className="w-full py-3 rounded-xl text-sm font-medium transition-all duration-200
                         disabled:opacity-40 disabled:cursor-not-allowed flex items-center justify-center gap-2"
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
