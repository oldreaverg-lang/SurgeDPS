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
// Throughput per adjuster per 8-hour day. These are deliberately
// conservative — roughly half of the raw industry rule-of-thumb
// numbers — so the Deployment Summary errs toward recommending
// bigger teams rather than overpromising on a single adjuster's
// capacity during a chaotic first-48-hour CAT response.
//
// The Phase 3 Deployment Planner (C3) will expose these as
// editable values in the UI, so a sophisticated user can tune
// them to match their own shop's throughput.
// ───────────────────────────────────────────────────────────
export const ADJUSTER_THROUGHPUT: Required<SeverityCounts> = {
  severe:    7, // per adjuster-day (raw rule-of-thumb ~15)
  major:    12, //                  (raw ~25)
  moderate: 20, //                  (raw ~40)
  minor:    30, //                  (raw ~60)
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

// ───────────────────────────────────────────────────────────
// §4b C3 — Deployment Planner ("X adjusters → Y days")
// §4b C5 — Time-to-Clear helpers
//
// Given a ranked list of hotspot areas (each with a severity mix
// that translates to a number of adjuster-days via recommendAdjusters),
// allocate a finite team budget (teamSize × windowDays) across the
// areas in priority order and report which areas are fully covered,
// partially covered, or not reachable within the window.
// ───────────────────────────────────────────────────────────

export type PlannerArea = {
  rank: number;
  label?: string;               // optional display name ("#1", "Parish A", etc.)
  severity: SeverityCounts;     // drives adjuster-day cost
};

export type AreaCoverage = {
  rank: number;
  label: string;
  required_days: number;        // adjuster-days needed for this area
  allocated_days: number;       // adjuster-days we assigned
  coverage_pct: number;         // 0–100
  status: 'covered' | 'partial' | 'uncovered';
};

export type DeploymentPlan = {
  team_size: number;
  window_days: number;
  capacity_adjuster_days: number;  // team_size * window_days
  required_adjuster_days: number;  // sum over all areas
  utilization_pct: number;         // min(100, capacity_used / capacity * 100)
  full_coverage: boolean;          // every area status === 'covered'
  coverage_pct: number;            // areas covered / total areas (whole number)
  areas: AreaCoverage[];
  shortfall_days: number;          // max(0, required - capacity)
};

/**
 * Allocate team capacity to areas in priority order.
 * Pure function — safe to call on every slider tick.
 */
export function planDeployment(
  areas: PlannerArea[],
  teamSize: number,
  windowDays: number,
): DeploymentPlan {
  const capacity = Math.max(0, teamSize) * Math.max(0, windowDays);
  let remaining = capacity;
  let requiredTotal = 0;
  let fullyCoveredCount = 0;

  const resolved: AreaCoverage[] = areas.map(a => {
    const required = adjusterDaysNeeded(a.severity);
    requiredTotal += required;
    const allocated = Math.min(required, remaining);
    remaining -= allocated;
    const coverage_pct = required > 0 ? Math.round((allocated / required) * 100) : 100;
    let status: AreaCoverage['status'];
    if (required <= 0.001) status = 'covered';
    else if (allocated >= required - 0.001) status = 'covered';
    else if (allocated > 0) status = 'partial';
    else status = 'uncovered';
    if (status === 'covered') fullyCoveredCount += 1;
    return {
      rank: a.rank,
      label: a.label ?? `#${a.rank}`,
      required_days: required,
      allocated_days: allocated,
      coverage_pct,
      status,
    };
  });

  const utilization_pct = capacity > 0 ? Math.min(100, Math.round((Math.min(capacity, requiredTotal) / capacity) * 100)) : 0;
  const full_coverage = resolved.length > 0 && resolved.every(a => a.status === 'covered');
  const coverage_pct = resolved.length > 0 ? Math.round((fullyCoveredCount / resolved.length) * 100) : 0;
  const shortfall_days = Math.max(0, requiredTotal - capacity);

  return {
    team_size: teamSize,
    window_days: windowDays,
    capacity_adjuster_days: capacity,
    required_adjuster_days: requiredTotal,
    utilization_pct,
    full_coverage,
    coverage_pct,
    areas: resolved,
    shortfall_days,
  };
}

/**
 * Back-solve: given a fixed window, how many adjusters do we need
 * to fully cover every area in `areas`? Rounded up to a whole
 * adjuster. Returns 0 if there is no work required.
 */
export function suggestTeamSize(areas: PlannerArea[], windowDays: number): number {
  const d = Math.max(1, windowDays);
  const total = areas.reduce((s, a) => s + adjusterDaysNeeded(a.severity), 0);
  if (total <= 0) return 0;
  return Math.max(1, Math.ceil(total / d));
}

/**
 * Storm-wide time-to-clear in days for a given team size.
 * Returns Infinity if teamSize <= 0.
 */
export function timeToClearDays(areas: PlannerArea[], teamSize: number): number {
  if (teamSize <= 0) return Infinity;
  const total = areas.reduce((s, a) => s + adjusterDaysNeeded(a.severity), 0);
  if (total <= 0) return 0;
  return total / teamSize;
}

/**
 * Pretty-print a day count as "~3 days" / "~1 week" / "~2 weeks".
 * Ops Mode never shows fractions of a day.
 */
export function formatTimeToClear(days: number): string {
  if (!isFinite(days)) return '—';
  if (days <= 0) return 'No work';
  const d = Math.ceil(days);
  if (d <= 1) return '~1 day';
  if (d <= 10) return `~${d} days`;
  const weeks = Math.round(d / 7);
  if (weeks <= 1) return '~1 week';
  return `~${weeks} weeks`;
}
