// ─────────────────────────────────────────────────────────────────────────────
// Display formatting utilities.
// Mode-aware (analyst vs ops) so components don't need to carry that logic.
// ─────────────────────────────────────────────────────────────────────────────

import type { DisplayMode, StormInfo } from '../types';

// ── String helpers ────────────────────────────────────────────────────────────

/** Strip the "Hurricane / Tropical Storm / Tropical Depression" prefix. */
export const shortName = (name: string): string =>
  name.replace(/^(Hurricane|Tropical Storm|Tropical Depression)\s+/i, '');

/** Sort descending by DPS score. */
export const byDPS = (a: StormInfo, b: StormInfo): number =>
  (b.dps_score || 0) - (a.dps_score || 0);

/** Escape a value for CSV output. */
export const csvField = (v: any): string => {
  const s = String(v ?? '');
  return s.includes(',') || s.includes('"') || s.includes('\n')
    ? `"${s.replace(/"/g, '""')}"`
    : s;
};

// ── Loss formatting ───────────────────────────────────────────────────────────

/**
 * Analyst Mode: full precision with units suffix.
 * Ops Mode: rounds aggressively to confident buckets — "$80M" not "$81.3M".
 */
export function formatLossOps(usd: number, mode: DisplayMode): string {
  if (!isFinite(usd) || usd <= 0) return '—';
  if (mode === 'analyst') {
    if (usd >= 1e9) return `$${(usd / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`;
    if (usd >= 1e6) return `$${(usd / 1e6).toLocaleString(undefined, { maximumFractionDigits: 1 })}M`;
    if (usd >= 1e3) return `$${(usd / 1e3).toLocaleString(undefined, { maximumFractionDigits: 0 })}K`;
    return `$${Math.round(usd).toLocaleString()}`;
  }
  if (usd >= 1e9) {
    const b = usd / 1e9;
    if (b >= 10) return `~$${Math.round(b)}B`;
    return `~$${(Math.round(b * 2) / 2).toFixed(1)}B`;
  }
  if (usd >= 1e8) return `~$${Math.round(usd / 1e8) * 100}M`;
  if (usd >= 1e7) return `~$${Math.round(usd / 1e7) * 10}M`;
  if (usd >= 1e6) return `~$${Math.round(usd / 1e6)}M`;
  if (usd >= 1e5) return `~$${Math.round(usd / 1e5) * 100}K`;
  return '<$1M';
}

export function formatCountOps(n: number, mode: DisplayMode): string {
  if (!isFinite(n) || n <= 0) return '0';
  if (mode === 'analyst') return Math.round(n).toLocaleString();
  if (n >= 100_000) return `~${Math.round(n / 1000).toLocaleString()}k`;
  if (n >= 10_000)  return `~${(Math.round(n / 100) / 10).toFixed(1)}k`;
  if (n >= 1_000)   return `~${Math.round(n / 100) * 100}`;
  if (n >= 100)     return `~${Math.round(n / 10) * 10}`;
  return Math.round(n).toLocaleString();
}

export function formatDepthOps(ft: number | null | undefined, mode: DisplayMode): string {
  if (ft == null || !isFinite(ft)) return '—';
  if (mode === 'analyst') return `${ft.toFixed(1)} ft`;
  return `~${Math.round(ft)} ft`;
}

// ── DPS color scale ───────────────────────────────────────────────────────────

export const dpsColor = (score: number): string => {
  if (score >= 80) return '#ef4444';
  if (score >= 60) return '#f97316';
  if (score >= 40) return '#fbbf24';
  if (score >= 20) return '#34d399';
  if (score >= 10) return '#60a5fa';
  return '#94a3b8';
};
