// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CAT Team / Emergency Manager helpers (pure functions)
//
// Phase 2 of CAT_TEAM_PLAN:
//   §4b C1  — adjuster recommendation heuristic
//   §4b C2  — NFIP vs HO3 routing hint
//   §4a B3  — severity → workload translation
//   §4a B2  — peril mix aggregation helpers
//
// These are intentionally pure functions — no React, no DOM, no
// network — so they can be unit-tested in isolation once we stand
// up a test harness, and so the UI layer stays thin.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

export type SeverityCounts = {
  severe?: number;
  major?: number;
  moderate?: number;
  minor?: number;
  none?: number;
};

// ───────────────────────────────────────────────────────────
// §4b C1 — Adjuster recommendation heuristic
//
// Industry rule-of-thumb throughput per adjuster per 8-hour day.
// These numbers are editable per-user in the Phase 3 Deployment
// Planner (C3). Values are deliberately conservative.
// ───────────────────────────────────────────────────────────
export const ADJUSTER_THROUGHPUT: Required<SeverityCounts> = {
  severe:   15, // per adjuster-day
  major:    25,
  moderate: 40,
  minor:    60,
  none:      0, // "none" inspections not counted
};

/** Total adjuster-days needed to clear the severity mix. */
export function adjusterDaysNeeded(counts: SeverityCounts): number {
  let days = 0;
  (Object.keys(ADJUSTER_THROUGHPUT) as (keyof SeverityCounts)[]).forEach(k => {
    const n = counts[k] || 0;
    const rate = ADJUSTER_THROUGHPUT[k];
    if (rate > 0) days += n / rate;
  });
  return days;
}

export type AdjusterRecommendation = {
  adjuster_days: number;
  adjusters: number;
  days: number;
  label: string; // "12 adjusters · 5 days"
};

/**
 * Recommend a reasonable (adjusters, days) pairing for a severity mix.
 *
 * Strategy: compute total adjuster-days, then prefer pairings that
 * sit inside a max-team-size / max-window-days box with reasonably
 * round numbers, leaning toward "typical" CAT deployments (teams of
 * 5–40, windows of 3–10 days).
 */
export function recommendAdjusters(
  counts: SeverityCounts,
  opts: { maxTeam?: number; maxDays?: number } = {},
): AdjusterRecommendation {
  const maxTeam = opts.maxTeam ?? 40;
  const maxDays = opts.maxDays ?? 10;
  const ad = adjusterDaysNeeded(counts);

  if (ad <= 0.001) {
    return { adjuster_days: 0, adjusters: 0, days: 0, label: 'No deployment needed' };
  }

  // Try a nominal 5-day window first; if the resulting team is too
  // big, stretch the window until the team fits inside maxTeam.
  let days = 5;
  let adjusters = Math.ceil(ad / days);
  while (adjusters > maxTeam && days < maxDays) {
    days += 1;
    adjusters = Math.ceil(ad / days);
  }
  // If we're still above maxTeam, we're just reporting the uncapped
  // team size with a max-days window — the UI will flag this as
  // "overflow" in the Deployment Planner later.
  if (adjusters > maxTeam) {
    days = maxDays;
    adjusters = Math.ceil(ad / days);
  }

  // Round very small teams up to at least 2 for realism.
  if (adjusters < 2 && ad > 0) adjusters = 2;

  const label = `${adjusters} adjuster${adjusters === 1 ? '' : 's'} · ${days} day${days === 1 ? '' : 's'}`;
  return { adjuster_days: ad, adjusters, days, label };
}

// ───────────────────────────────────────────────────────────
// §4b C2 — Claims routing hint (NFIP vs HO3 vs dual-route)
// ───────────────────────────────────────────────────────────
export type RoutingHint = 'nfip' | 'ho3' | 'mixed';

export type RoutingTag = {
  hint: RoutingHint;
  label: string;
  short: string;
  description: string;
  // Tailwind classes for a small pill — kept here so the UI doesn't
  // have to re-derive them in three places.
  classes: string;
};

export function routingHint(windPct: number, waterPct: number): RoutingTag {
  if (waterPct >= 70) {
    return {
      hint: 'nfip',
      label: 'NFIP primary',
      short: 'NFIP',
      description: 'Flood carrier leads — route to NFIP adjusters first',
      classes: 'bg-indigo-100 text-indigo-800 border border-indigo-200',
    };
  }
  if (windPct >= 70) {
    return {
      hint: 'ho3',
      label: 'HO3 primary',
      short: 'HO3',
      description: 'Standard homeowners carrier leads — wind-driven losses',
      classes: 'bg-sky-100 text-sky-800 border border-sky-200',
    };
  }
  return {
    hint: 'mixed',
    label: 'Mixed — dual-route',
    short: 'Mixed',
    description: 'Both flood and wind carriers in play — coordinate dual adjustment',
    classes: 'bg-purple-100 text-purple-800 border border-purple-200',
  };
}

// ───────────────────────────────────────────────────────────
// §4a B3 — Severity → workload translation
// ───────────────────────────────────────────────────────────
export type WorkloadSummary = {
  inspections_needed: number;   // severe + major + moderate + minor
  uninhabitable: number;        // severe + major
  summary: string;              // e.g. "8,200 inspections · 1,400 likely uninhabitable"
  headline: string;             // short verdict for a panel header
};

export function workloadSummary(counts: SeverityCounts): WorkloadSummary {
  const severe   = counts.severe   || 0;
  const major    = counts.major    || 0;
  const moderate = counts.moderate || 0;
  const minor    = counts.minor    || 0;
  const inspections_needed = severe + major + moderate + minor;
  const uninhabitable = severe + major;

  const fmt = (n: number) => n.toLocaleString();
  let summary = '';
  if (inspections_needed === 0) {
    summary = 'No inspections needed';
  } else if (uninhabitable > 0) {
    summary = `${fmt(inspections_needed)} inspections · ~${fmt(uninhabitable)} likely uninhabitable`;
  } else {
    summary = `${fmt(inspections_needed)} inspections needed`;
  }

  let headline = 'Monitor';
  if (uninhabitable >= 1000)      headline = 'Deploy immediately';
  else if (uninhabitable >= 250)  headline = 'Deploy CAT team';
  else if (inspections_needed >= 500) headline = 'Deploy field adjusters';
  else if (inspections_needed > 0) headline = 'Standard claims handling';

  return { inspections_needed, uninhabitable, summary, headline };
}

// ───────────────────────────────────────────────────────────
// §4a B2 — Peril mix aggregation helpers
//
// Aggregate a list of per-hotspot { windPct, waterPct, count } rows
// into a single weighted split for the whole footprint.
// ───────────────────────────────────────────────────────────
export type PerilMix = { windPct: number; waterPct: number };

export function aggregatePerilMix(
  rows: Array<{ windPct: number; waterPct: number; weight: number }>,
): PerilMix {
  let w = 0, wWind = 0, wWater = 0;
  for (const r of rows) {
    if (r.weight <= 0) continue;
    w += r.weight;
    wWind  += r.windPct  * r.weight;
    wWater += r.waterPct * r.weight;
  }
  if (w === 0) return { windPct: 50, waterPct: 50 };
  const windPct = Math.round(wWind / w);
  return { windPct, waterPct: 100 - windPct };
}

export function perilHeadline(mix: PerilMix): string {
  if (mix.waterPct >= 70) return `Water-dominant event (${mix.waterPct}% water / ${mix.windPct}% wind)`;
  if (mix.windPct  >= 70) return `Wind-dominant event (${mix.windPct}% wind / ${mix.waterPct}% water)`;
  return `Mixed peril event (${mix.windPct}% wind / ${mix.waterPct}% water)`;
}
