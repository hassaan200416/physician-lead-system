# Physician Lead System Frontend

React application for browsing, filtering, and reviewing physician leads.

## What This App Does

- Displays lead pipeline metrics and distribution.
- Provides searchable/filterable lead tables.
- Shows lead details and review flows.
- Includes activity-focused views for recent lead additions.

## Tech Stack

- React 19
- TypeScript
- Vite
- React Router
- TanStack Query
- Supabase JS
- Tailwind CSS
- Framer Motion

## Project Structure

- src/components: reusable UI, leads, and layout components
- src/pages: route-level pages (Dashboard, Leads, Activity)
- src/hooks: data access hooks
- src/lib: clients and utilities
- src/types: shared TypeScript models

## Prerequisites

- Node.js 20+
- npm 10+
- Supabase project URL and anon key

## Environment Variables

Create frontend/.env.local:

```env
VITE_SUPABASE_URL=<your_supabase_project_url>
VITE_SUPABASE_ANON_KEY=<your_supabase_anon_key>
```

Notes:

- Variable names must use the VITE\_ prefix.
- Do not commit secrets or non-public keys.

## Local Setup

From repository root:

```bash
cd frontend
npm install
```

Start development server:

```bash
npm run dev
```

Build for production:

```bash
npm run build
```

Preview production build:

```bash
npm run preview
```

Run linting:

```bash
npm run lint
```

## Routing

Current routes in src/App.tsx:

- /
- /leads
- /activity

## Data Layer

- Supabase client is initialized in src/lib/supabase.ts.
- Query hooks live in src/hooks and use TanStack Query for caching and loading states.
- UI components should consume hooks instead of calling Supabase directly where possible.

## UX and Component Practices

- Keep page components focused on orchestration.
- Keep reusable logic in hooks and pure helpers.
- Prefer typed props and shared types in src/types.
- Keep visual tokens and shared utility classes consistent.

## Production Practices

- Inject environment variables at deploy time.
- Use CI checks for lint and build before merge.
- Enable source map handling and error monitoring in production.
- Serve static build via CDN or edge hosting.
- Restrict Supabase RLS policies to minimum required access.

## Troubleshooting

Blank page or data errors:

- Verify frontend/.env.local exists and has valid VITE values.
- Restart dev server after changing environment variables.

Build fails on type errors:

- Run npm run lint and address warnings/errors before build.

Supabase request errors:

- Confirm project URL/key pair and table/RLS configuration.
