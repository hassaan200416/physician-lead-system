import { motion } from "framer-motion";
import { Users, Star, TrendingUp, ShieldCheck } from "lucide-react";
import { StatCard } from "../components/ui/StatCard";
import { TopBar } from "../components/layout/TopBar";
import { useStats } from "../hooks/useStats";

export function Dashboard() {
  const { data: stats, isLoading } = useStats();

  if (isLoading)
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-6 h-6 border-2 border-teal-500 border-t-transparent rounded-full animate-spin" />
      </div>
    );

  return (
    <div>
      <TopBar title="Dashboard" />
      <div className="p-6 space-y-6">
        <div>
          <h2 className="text-xs text-slate-500 uppercase tracking-widest mb-4">
            Lead Pipeline Overview
          </h2>
          <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3">
            <StatCard
              label="Total Leads"
              value={stats?.total ?? 0}
              icon={Users}
              delay={0}
            />
            <StatCard
              label="Tier A"
              value={stats?.tier_a ?? 0}
              icon={Star}
              accent="#10b981"
              delay={0.05}
            />
            <StatCard
              label="Tier B"
              value={stats?.tier_b ?? 0}
              icon={TrendingUp}
              accent="#2dd4bf"
              delay={0.1}
            />
            <StatCard
              label="Tier C"
              value={stats?.tier_c ?? 0}
              icon={TrendingUp}
              accent="#f59e0b"
              delay={0.15}
            />
            <StatCard
              label="High Confidence"
              value={stats?.high_confidence ?? 0}
              icon={ShieldCheck}
              accent="#818cf8"
              delay={0.2}
            />
            <StatCard
              label="Avg Score"
              value={`${stats?.avg_score ?? 0}`}
              icon={TrendingUp}
              accent="#f472b6"
              delay={0.25}
            />
          </div>
        </div>

        <div className="glass rounded-xl p-6">
          <h2 className="text-xs text-slate-500 uppercase tracking-widest mb-4">
            Tier Distribution
          </h2>
          <div className="space-y-3">
            {[
              {
                tier: "A",
                label: "Tier A — High Priority",
                count: stats?.tier_a ?? 0,
                color: "bg-emerald-400",
              },
              {
                tier: "B",
                label: "Tier B — Medium Priority",
                count: stats?.tier_b ?? 0,
                color: "bg-teal-400",
              },
              {
                tier: "C",
                label: "Tier C — Low Priority",
                count: stats?.tier_c ?? 0,
                color: "bg-amber-400",
              },
            ].map(({ tier, label, count, color }) => {
              const total = stats?.total || 1;
              const pct = Math.round((count / total) * 100);

              return (
                <div key={tier} className="flex items-center gap-4">
                  <span className="text-xs text-slate-400 w-36">{label}</span>
                  <div className="flex-1 h-2 bg-white/5 rounded-full overflow-hidden">
                    <motion.div
                      className={`h-full ${color} rounded-full`}
                      initial={{ width: 0 }}
                      animate={{ width: `${pct}%` }}
                      transition={{ duration: 0.8, delay: 0.3 }}
                    />
                  </div>
                  <span className="text-xs font-mono text-slate-400 w-16 text-right">
                    {count} ({pct}%)
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
