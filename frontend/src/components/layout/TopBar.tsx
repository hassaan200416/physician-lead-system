import { Search } from "lucide-react";

interface TopBarProps {
  title: string;
  search?: string;
  onSearch?: (val: string) => void;
}

export function TopBar({ title, search, onSearch }: TopBarProps) {
  return (
    <header
      className="h-14 flex items-center justify-between px-6
                       border-b border-white/5 bg-navy-950/80 backdrop-blur-sm sticky top-0 z-40"
    >
      <h1 className="font-display font-semibold text-white text-base tracking-tight">
        {title}
      </h1>

      {onSearch !== undefined && (
        <div className="relative">
          <Search
            size={14}
            className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500"
          />
          <input
            type="text"
            placeholder="Search leads..."
            value={search}
            onChange={(e) => onSearch(e.target.value)}
            className="bg-white/5 border border-white/10 rounded-lg pl-8 pr-4 py-1.5
                       text-sm text-slate-300 placeholder:text-slate-600
                       focus:outline-none focus:border-teal-500/50 focus:bg-white/8
                       transition-all w-56"
          />
        </div>
      )}
    </header>
  );
}
