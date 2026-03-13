import { NavLink } from "react-router-dom";
import { LayoutDashboard, Users, Activity } from "lucide-react";

const links = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/leads", icon: Users, label: "Leads" },
  { to: "/activity", icon: Activity, label: "Activity" },
];

export function Sidebar() {
  return (
    <aside
      className="fixed left-0 top-0 h-screen w-16 flex flex-col items-center py-6 gap-2
                      bg-navy-950 border-r border-white/5 z-50"
    >
      {/* Logo mark */}
      <div
        className="w-8 h-8 rounded-lg bg-teal-500/20 border border-teal-500/40
                      flex items-center justify-center mb-6"
      >
        <span className="text-teal-400 font-mono font-bold text-sm">P</span>
      </div>

      {links.map(({ to, icon: Icon, label }) => (
        <NavLink
          key={to}
          to={to}
          end={to === "/"}
          title={label}
          className={({ isActive }) => `
            w-10 h-10 rounded-xl flex items-center justify-center
            transition-all duration-200 group relative
            ${
              isActive
                ? "bg-teal-500/20 text-teal-400 border border-teal-500/30"
                : "text-slate-500 hover:text-slate-300 hover:bg-white/5"
            }
          `}
        >
          <Icon size={18} />
          <span
            className="absolute left-14 bg-navy-800 text-white text-xs px-2 py-1
                           rounded-md opacity-0 group-hover:opacity-100 transition-opacity
                           whitespace-nowrap pointer-events-none border border-white/10"
          >
            {label}
          </span>
        </NavLink>
      ))}
    </aside>
  );
}
