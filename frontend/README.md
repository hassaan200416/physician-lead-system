# Frontend — Physician Lead System

React + TypeScript dashboard for browsing, filtering, and reviewing physician leads.

---

## Table of Contents

1. [Stack](#stack)
2. [Directory Structure](#directory-structure)
3. [Local Setup](#local-setup)
4. [Environment Variables](#environment-variables)
5. [Running the App](#running-the-app)
6. [Pages and Features](#pages-and-features)
7. [Data Layer](#data-layer)
8. [Key Data Concepts](#key-data-concepts)
9. [Component Architecture](#component-architecture)
10. [Styling](#styling)
11. [Troubleshooting](#troubleshooting)

---

## Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| React | 19 | UI framework |
| TypeScript | 5.9 | Type safety |
| Vite | 8.0 | Build tool and dev server |
| React Router | 7 | Client-side routing |
| TanStack Query | 5 | Data fetching, caching, loading states |
| Supabase JS | 2 | Direct PostgreSQL reads via REST |
| Tailwind CSS | 3.4 | Utility-first styling |
| Framer Motion | 12 | Animations |
| lucide-react | — | Icons |
| Recharts | 3 | Charts (available, used on Dashboard) |

---

## Directory Structure

```
frontend/
├── src/
│   ├── pages/
│   │   ├── Dashboard.tsx       # Pipeline metrics overview
│   │   ├── Leads.tsx           # Filterable, searchable lead table
│   │   └── Activity.tsx        # Recently added + recently reviewed feeds
│   │
│   ├── components/
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx     # Fixed left navigation bar
│   │   │   └── TopBar.tsx      # Sticky page header + search input
│   │   ├── leads/
│   │   │   ├── LeadTable.tsx   # Table wrapper + header row
│   │   │   ├── LeadRow.tsx     # Single animated table row
│   │   │   ├── LeadFilters.tsx # Category / confidence / state filter bar
│   │   │   ├── LeadDrawer.tsx  # Slide-in detail panel
│   │   │   └── ReviewModal.tsx # Rating + status + notes submission modal
│   │   └── ui/
│   │       ├── Badge.tsx       # Pill label (tier, category, confidence)
│   │       ├── ScoreRing.tsx   # Circular SVG score indicator
│   │       └── StatCard.tsx    # Animated metric tile
│   │
│   ├── hooks/
│   │   ├── useLeads.ts         # Paginated, filtered lead query
│   │   ├── useStats.ts         # Aggregate dashboard stats
│   │   └── useReviews.ts       # Review fetch + upsert (single + all)
│   │
│   ├── lib/
│   │   ├── supabase.ts         # Supabase client initialisation
│   │   └── utils.ts            # Color helpers (tier, category, confidence, score)
│   │
│   ├── types/
│   │   └── index.ts            # Lead, LeadStats, LeadReview, filter types
│   │
│   ├── App.tsx                 # Router setup + QueryClient provider
│   └── main.tsx                # React DOM entry point
│
├── public/
│   └── favicon.svg
│
├── .env.local                  # Supabase credentials — never commit
├── package.json
├── vite.config.ts
├── tailwind.config.js
└── tsconfig.app.json
```

---

## Local Setup

### Prerequisites

- Node.js 20 or higher
- npm 10 or higher

Check your versions:
```bash
node --version
npm --version
```

### Install dependencies

```bash
cd frontend
npm install
```

---

## Environment Variables

Create `frontend/.env.local`. This file is gitignored — never commit it.

```env
VITE_SUPABASE_URL=https://<your-project-id>.supabase.co
VITE_SUPABASE_ANON_KEY=<your-supabase-anon-key>
```

Get these values from your Supabase project dashboard under **Project Settings → API**.

> **Why is the anon key safe to use in the browser?**
> The anon key only grants access that Supabase Row Level Security (RLS) policies permit. It is not a secret — it is the intended public key for client-side apps. Access control happens on the database side, not by hiding this key.

**Important:** Variable names must start with `VITE_` for Vite to expose them to the browser. If you rename them, the app will throw an error at startup.

After creating or changing `.env.local`, restart the dev server.

---

## Running the App

```bash
# Start development server with hot reload
npm run dev
```

The app will be available at **http://localhost:5173**.

```bash
# Build for production (output goes to dist/)
npm run build

# Preview the production build locally
npm run preview

# Run ESLint
npm run lint
```

---

## Pages and Features

### Dashboard (`/`)

- Six stat cards: Total Leads, Tier A count, Tier B count, Tier C count, High Confidence count, Average Score
- Animated tier distribution bar chart (A/B/C proportions)
- Data from `useStats()` hook — queries the `leads` table directly via Supabase

### Leads (`/leads`)

- Full leads table sorted by lead score descending
- **Category filter** — A (phone + email), B (email only), or ALL
- **Confidence filter** — HIGH, MEDIUM, or ALL (checks `personal_email_confidence` first, falls back to legacy `email_confidence_level`)
- **State filter** — dropdown populated from the current result set
- **Search** — full-text across first name, last name, organisation, and both email columns
- **Pagination** — 50 records per page
- **Drawer** — click any row to open the detail panel (contact, practice, professional, data sources sections)
- **Review modal** — rate 1–10, assign a status (Contacted/Interested/etc.), add notes; saves to `lead_reviews` table

### Activity (`/activity`)

- Left panel: 10 most recently added leads (ordered by `created_at`)
- Right panel: all reviews sorted by `updated_at` descending

---

## Data Layer

### How the frontend connects to the database

The frontend reads data in two ways:

1. **Supabase JS client (direct)** — `src/lib/supabase.ts` connects directly to the `leads` and `lead_reviews` tables. This is the primary read path for all three pages.

2. **FastAPI REST API** — used for operations not available in the leads table directly (exports, sync logs, call outcomes). The backend must be running on port 8000 for these features.

### Hooks

All data fetching goes through TanStack Query hooks. Never call Supabase or the API directly from a component.

| Hook | File | What it queries |
|------|------|----------------|
| `useLeads()` | `hooks/useLeads.ts` | `leads` table with filters + pagination |
| `useStats()` | `hooks/useStats.ts` | `leads` table, derives counts client-side |
| `useReview(npi)` | `hooks/useReviews.ts` | Single row from `lead_reviews` |
| `useUpsertReview()` | `hooks/useReviews.ts` | Mutation — upserts to `lead_reviews` |
| `useAllReviews()` | `hooks/useReviews.ts` | All rows from `lead_reviews` |

**Cache times:**
- `useLeads` — 30 seconds (re-fetches on filter or page change)
- `useStats` — 60 seconds

### The `leads` table

This is a denormalised flat table written by the backend enrichment scripts. It contains only physicians with at least one contact signal. Columns include everything the frontend needs without joins:

```
npi, first_name, last_name, full_name, credential, specialty, specialty_category,
organization_name, practice_domain,
personal_email, personal_email_confidence,
email (legacy), email_confidence_level (legacy),
practice_email,
mobile_phone, phone_confidence,
contact_category,        ← A (phone+email) or B (email only)
enrichment_sources[],
address_line_1, city, state, zip,
lead_score, lead_tier,   ← score-based tier (A/B/C/Archive)
years_of_experience, experience_bucket,
license_count, multi_state_flag,
created_at, updated_at
```

---

## Key Data Concepts

### `contact_category` vs `lead_tier`

These are two different things that both use A/B labels — do not confuse them.

| Field | Type | Values | Meaning |
|-------|------|--------|---------|
| `contact_category` | Contact-based | A, B | A = has phone + email; B = email only |
| `lead_tier` | Score-based | A, B, C, Archive | Reflects numeric lead score (A = 80–100) |

The UI filters by `contact_category`. The score ring and tier badge show `lead_tier`. See `src/types/index.ts` for the full documentation.

### Email columns — which one to display

There are three email columns. Always use this priority order:

```typescript
const displayEmail = lead.personal_email || lead.email
const displayConfidence = lead.personal_email_confidence || lead.email_confidence_level
```

- `personal_email` — written by all enrichment tools (Hunter v2+, Apollo, ContactOut, etc.)
- `email` — legacy, written by the original Hunter v1 pipeline; **read-only going forward**
- `practice_email` — sourced from NPPES; useful as a fallback but less reliable for outreach

### `enrichment_sources`

A PostgreSQL text array stored on each lead, e.g. `["hunter.io", "fullenrich"]`. Tells you which tools found data for this lead. Displayed in the Drawer's "Data Sources" section.

---

## Component Architecture

```
App.tsx
└── Sidebar (fixed left nav)
└── main
    ├── Dashboard
    │   ├── TopBar
    │   └── StatCard × 6
    │
    ├── Leads
    │   ├── TopBar (with search)
    │   ├── LeadFilters (category / confidence / state)
    │   ├── LeadTable
    │   │   └── LeadRow × N
    │   ├── Pagination
    │   ├── LeadDrawer (conditionally rendered)
    │   └── ReviewModal (conditionally rendered)
    │
    └── Activity
        ├── TopBar
        ├── Recent Leads panel (LeadRow-style cards)
        └── Recent Reviews panel
```

**Design rules:**
- Pages own state and wire hooks to components via props
- Components are purely presentational — they receive data and callbacks, they do not call hooks themselves (except for `ReviewModal` which uses `useReview` and `useUpsertReview` internally)
- All colour logic is in `src/lib/utils.ts` (`getCategoryColor`, `getTierColor`, `getConfidenceColor`, `getScoreColor`) — use these helpers rather than hardcoding Tailwind colour strings per component

---

## Styling

Tailwind CSS with custom theme extensions defined in `tailwind.config.js`:

| Token | Value | Usage |
|-------|-------|-------|
| `navy-950` | `#0a0e1a` | Page background |
| `navy-900` | `#0d1424` | Card/drawer background |
| `navy-800` | `#111827` | Tooltip/dropdown background |
| `glass` | Custom class | Frosted glass card effect |
| `glass-hover` | Custom class | Hover state for glass cards |

The font stack uses `font-display` (set to `Inter` or system-ui) for headings and `font-mono` for NPI numbers, scores, and email addresses.

---

## Troubleshooting

**Blank page / app does not load**
→ Open browser DevTools console. If you see `Missing Supabase environment variables`, your `frontend/.env.local` file is missing or has wrong variable names. Ensure they start with `VITE_`.

**Data shows as empty / loading forever**
→ Check the Network tab for failing Supabase requests. Common causes:
- Wrong `VITE_SUPABASE_URL` or `VITE_SUPABASE_ANON_KEY`
- The `leads` table is empty (backend enrichment has not run yet)
- Supabase RLS is blocking the anon key from reading the table

**"No leads match your filters"**
→ The leads table may genuinely be empty, or your filters are too restrictive. Try resetting all filters (Category = ALL, Confidence = ALL, no state selected, no search text).

**Changes to `.env.local` not taking effect**
→ Vite reads environment variables at build time. Stop the dev server and restart it with `npm run dev`.

**Build fails with TypeScript errors**
→ Run `npm run lint` first. Fix type errors before attempting a build.

**Review modal shows a spinner indefinitely**
→ The `lead_reviews` table may not exist in Supabase, or RLS is blocking the anon key from reading/writing it. Check the Supabase dashboard → Table Editor.
