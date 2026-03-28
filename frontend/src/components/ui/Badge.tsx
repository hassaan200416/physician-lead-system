/**
 * Badge.tsx
 * ---------
 * Pill-shaped label for tier, category, and confidence values.
 * Caller supplies color classes via `className`; use the helper
 * functions getCategoryColor / getTierColor / getConfidenceColor
 * from lib/utils.ts to keep colors consistent.
 */
interface BadgeProps {
  children: React.ReactNode;
  /** Tailwind colour classes applied to the badge (text, bg, border). */
  className?: string;
}

export function Badge({ children, className = "" }: BadgeProps) {
  return (
    <span
      className={`
      inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium
      border font-mono tracking-wide ${className}
    `}
    >
      {children}
    </span>
  );
}
