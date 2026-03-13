import { useState, useRef, useEffect } from "react";
import { ChevronDown, MapPin } from "lucide-react";
import type { TierFilter, ConfidenceFilter } from "../../types";

interface LeadFiltersProps {
  tier: TierFilter;
  confidence: ConfidenceFilter;
  state: string;
  states: string[];
  onTier: (v: TierFilter) => void;
  onConfidence: (v: ConfidenceFilter) => void;
  onState: (v: string) => void;
}

const tiers: TierFilter[] = ["ALL", "A", "B", "C"];
const confidences: ConfidenceFilter[] = ["ALL", "HIGH", "MEDIUM"];

const tierColors: Record<TierFilter, string> = {
  ALL: "text-slate-300 border-slate-600 bg-slate-600/10",
  A: "text-emerald-400 border-emerald-500/40 bg-emerald-500/10",
  B: "text-teal-400 border-teal-500/40 bg-teal-500/10",
  C: "text-amber-400 border-amber-500/40 bg-amber-500/10",
};

interface CustomDropdownProps {
  value: string;
  options: string[];
  onChange: (v: string) => void;
}

function CustomDropdown({ value, options, onChange }: CustomDropdownProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

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
      <button
        onClick={() => setOpen((isOpen) => !isOpen)}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs
                   bg-white/5 border border-white/10 text-slate-300
                   hover:border-teal-500/40 hover:text-white
                   transition-all duration-150 min-w-[110px]"
      >
        <MapPin size={11} className="text-slate-500" />
        <span className="flex-1 text-left">{value || "All States"}</span>
        <ChevronDown
          size={12}
          className={`text-slate-500 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
        />
      </button>

      {open && (
        <div
          className="absolute top-full mt-1.5 left-0 z-50 min-w-[130px]
                        bg-navy-900 border border-white/10 rounded-xl
                        shadow-xl shadow-black/40 overflow-hidden"
        >
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

      <CustomDropdown value={state} options={states} onChange={onState} />
    </div>
  );
}
