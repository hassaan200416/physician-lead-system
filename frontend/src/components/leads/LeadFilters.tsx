/**
 * LeadFilters.tsx
 * ---------------
 * Filter bar rendered above the leads table.
 * Contains three filter controls: Tier, Confidence, and State.
 *
 * All filters are controlled — parent (Leads.tsx) owns state
 * and passes current values + change handlers as props.
 *
 * State dropdown uses a custom-built overlay (CustomDropdown)
 * instead of the native <select> element for consistent dark
 * theme styling. It detects outside clicks via a ref to auto-close.
 */

import { useState, useRef, useEffect } from "react";
import { ChevronDown, MapPin } from "lucide-react";
import type { TierFilter, ConfidenceFilter } from "../../types";

interface LeadFiltersProps {
  /** Currently active tier filter. */
  tier: TierFilter;
  /** Currently active confidence filter. */
  confidence: ConfidenceFilter;
  /** Currently selected state code, or empty string for all. */
  state: string;
  /** List of state codes derived from current leads data. */
  states: string[];
  /** Called when tier filter button is clicked. */
  onTier: (v: TierFilter) => void;
  /** Called when confidence filter button is clicked. */
  onConfidence: (v: ConfidenceFilter) => void;
  /** Called when a state is selected from the dropdown. */
  onState: (v: string) => void;
}

const tiers: TierFilter[] = ["ALL", "A", "B", "C"];
const confidences: ConfidenceFilter[] = ["ALL", "HIGH", "MEDIUM"];

/** Active state color classes per tier — applied when that tier is selected. */
const tierColors: Record<TierFilter, string> = {
  ALL: "text-slate-300 border-slate-600 bg-slate-600/10",
  A: "text-emerald-400 border-emerald-500/40 bg-emerald-500/10",
  B: "text-teal-400 border-teal-500/40 bg-teal-500/10",
  C: "text-amber-400 border-amber-500/40 bg-amber-500/10",
};

interface CustomDropdownProps {
  /** Currently selected state code, or empty string for "All States". */
  value: string;
  /** Available state options to display. */
  options: string[];
  /** Called with the new state value when an option is selected. */
  onChange: (v: string) => void;
}

/**
 * Fully custom styled dropdown replacing native <select>.
 *
 * Uses a ref + mousedown event listener to detect clicks outside
 * the component and close the dropdown automatically.
 *
 * The chevron icon rotates 180° when open via a CSS class toggle.
 * Active selection is highlighted in teal.
 */
function CustomDropdown({ value, options, onChange }: CustomDropdownProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close dropdown when user clicks anywhere outside the component
  useEffect(() => {
    function handleClick(event: MouseEvent) {
      if (ref.current && !ref.current.contains(event.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div ref={ref} className="relative">
      {/* Trigger button — shows current selection or "All States" */}
      <button
        onClick={() => setOpen((isOpen) => !isOpen)}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs
                   bg-white/5 border border-white/10 text-slate-300
                   hover:border-teal-500/40 hover:text-white
                   transition-all duration-150 min-w-[110px]"
      >
        <MapPin size={11} className="text-slate-500" />
        <span className="flex-1 text-left">{value || "All States"}</span>
        {/* Chevron rotates when dropdown is open */}
        <ChevronDown
          size={12}
          className={`text-slate-500 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
        />
      </button>

      {/* Dropdown overlay — rendered below trigger when open */}
      {open && (
        <div
          className="absolute top-full mt-1.5 left-0 z-50 min-w-[130px]
                        bg-navy-900 border border-white/10 rounded-xl
                        shadow-xl shadow-black/40 overflow-hidden"
        >
          {/* "All States" option — clears state filter */}
          <button
            onClick={() => {
              onChange("");
              setOpen(false);
            }}
            className={`w-full text-left px-3 py-2 text-xs transition-colors
                        ${
                          !value
                            ? "text-teal-400 bg-teal-500/10"
                            : "text-slate-400 hover:text-white hover:bg-white/5"
                        }`}
          >
            All States
          </button>

          <div className="h-px bg-white/5 mx-2" />

          {/* Scrollable state list — max height prevents overflow */}
          <div className="max-h-48 overflow-y-auto">
            {options.map((option) => (
              <button
                key={option}
                onClick={() => {
                  onChange(option);
                  setOpen(false);
                }}
                className={`w-full text-left px-3 py-2 text-xs transition-colors
                            ${
                              value === option
                                ? "text-teal-400 bg-teal-500/10"
                                : "text-slate-400 hover:text-white hover:bg-white/5"
                            }`}
              >
                {option}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export function LeadFilters({
  tier,
  confidence,
  state,
  states,
  onTier,
  onConfidence,
  onState,
}: LeadFiltersProps) {
  return (
    <div className="flex items-center gap-4 px-6 py-3 border-b border-white/5 flex-wrap">
      {/* Tier filter buttons */}
      <div className="flex items-center gap-1.5">
        <span className="text-xs text-slate-500 uppercase tracking-wider mr-1">
          Tier
        </span>
        {tiers.map((currentTier) => (
          <button
            key={currentTier}
            onClick={() => onTier(currentTier)}
            className={`px-2.5 py-1 rounded-md text-xs font-mono font-medium border
                        transition-all duration-150
                        ${
                          tier === currentTier
                            ? tierColors[currentTier]
                            : "text-slate-500 border-white/5 hover:border-white/10 hover:text-slate-300"
                        }`}
          >
            {currentTier}
          </button>
        ))}
      </div>

      <div className="w-px h-4 bg-white/10" />

      {/* Confidence filter buttons */}
      <div className="flex items-center gap-1.5">
        <span className="text-xs text-slate-500 uppercase tracking-wider mr-1">
          Confidence
        </span>
        {confidences.map((currentConfidence) => (
          <button
            key={currentConfidence}
            onClick={() => onConfidence(currentConfidence)}
            className={`px-2.5 py-1 rounded-md text-xs font-medium border transition-all duration-150
                        ${
                          confidence === currentConfidence
                            ? "text-teal-400 border-teal-500/40 bg-teal-500/10"
                            : "text-slate-500 border-white/5 hover:border-white/10 hover:text-slate-300"
                        }`}
          >
            {currentConfidence}
          </button>
        ))}
      </div>

      <div className="w-px h-4 bg-white/10" />

      {/* Custom state dropdown */}
      <CustomDropdown value={state} options={states} onChange={onState} />
    </div>
  );
}
