export function getTierColor(tier: string): string {
  switch (tier) {
    case 'A': return 'text-emerald-400 bg-emerald-400/10 border-emerald-400/30'
    case 'B': return 'text-teal-400 bg-teal-400/10 border-teal-400/30'
    case 'C': return 'text-amber-400 bg-amber-400/10 border-amber-400/30'
    default:  return 'text-slate-400 bg-slate-400/10 border-slate-400/30'
  }
}

export function getConfidenceColor(level: string): string {
  switch (level) {
    case 'HIGH':   return 'text-emerald-400 bg-emerald-400/10 border-emerald-400/30'
    case 'MEDIUM': return 'text-amber-400 bg-amber-400/10 border-amber-400/30'
    case 'LOW':    return 'text-red-400 bg-red-400/10 border-red-400/30'
    default:       return 'text-slate-400 bg-slate-400/10 border-slate-400/30'
  }
}

export function getScoreColor(score: number): string {
  if (score >= 80) return '#10b981'
  if (score >= 60) return '#2dd4bf'
  if (score >= 40) return '#f59e0b'
  return '#6b7280'
}

export function formatScore(score: number): string {
  return score?.toFixed(1) ?? '0.0'
}