/**
 * App.tsx
 * -------
 * Root component. Provides the React Query client and router, then renders
 * the fixed left Sidebar alongside the main content area where pages mount.
 *
 * Routes:
 *   /          → Dashboard  (pipeline stats and tier distribution)
 *   /leads     → Leads      (filterable, searchable lead table)
 *   /activity  → Activity   (recently added + recently reviewed feeds)
 */
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Sidebar } from "./components/layout/Sidebar";
import { Dashboard } from "./pages/Dashboard";
import { Leads } from "./pages/Leads";
import { Activity } from "./pages/Activity";

const queryClient = new QueryClient();

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <div className="flex min-h-screen bg-navy-950">
          <Sidebar />
          <main className="flex-1 ml-16 min-h-screen">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/leads" element={<Leads />} />
              <Route path="/activity" element={<Activity />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
