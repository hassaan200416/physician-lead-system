/**
 * StatCard.tsx
 * ------------
 * Animated metric tile used in the Dashboard pipeline overview grid.
 * Fades and slides up on mount; delay staggers cards when rendered in a loop.
 */
import { motion } from "framer-motion";
import type { LucideIcon } from "lucide-react";

interface StatCardProps {
  label: string;
  value: number | string;
  icon: LucideIcon;
  /** Hex accent color for the icon background and icon itself. Default: teal. */
  accent?: string;
  /** Framer Motion mount delay in seconds. Stagger multiple cards by passing 0, 0.05, 0.1 … */
  delay?: number;
}

export function StatCard({
  label,
  value,
  icon: Icon,
  accent = "#2dd4bf",
  delay = 0,
}: StatCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay }}
      className="glass glass-hover rounded-xl p-5 cursor-default"
    >
      <div className="flex items-start justify-between mb-3">
        <div
          className="w-9 h-9 rounded-lg flex items-center justify-center"
          style={{ background: `${accent}18`, border: `1px solid ${accent}30` }}
        >
          <Icon size={16} style={{ color: accent }} />
        </div>
      </div>
      <div className="font-display font-bold text-2xl text-white mb-1">
        {value}
      </div>
      <div className="text-xs text-slate-400 font-medium tracking-wide uppercase">
        {label}
      </div>
    </motion.div>
  );
}
