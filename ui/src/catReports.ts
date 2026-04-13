// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CAT Deployment Report + Situation Report HTML builders
//
// Phase 3 of CAT_TEAM_PLAN:
//   §4b C4  — CAT Deployment Report (HTML download, reuses the
//             same "build a string, Blob it, download" pattern as
//             the existing Claims Documentation Report)
//   §4a B8  — Situation Report (same pipeline, EM-focused template)
//
// Both return a full HTML document string. The caller is
// responsible for Blob'ing it, URL.createObjectURL'ing it, and
// triggering the download. That lets the UI layer own the DOM
// side-effects and keeps this module pure / testable.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import {
  aggregatePerilMix,
  perilHeadline,
  workloadSummary,
  planDeployment,
  timeToClearDays,
  formatTimeToClear,
  worstShelterPosture,
  shelterPosture,
  lossRoutingSplit,
  hazardMechanismLabel,
} from './catTeam';
import type { RoutingTag, AdjusterRecommendation, HazardMix, LossRoutingSplit } from './catTeam';

// Shape we expect from App.tsx's Hotspot interface, narrowed to
// just the fields the reports touch so this module doesn't have
// to depend on App.tsx's types.
export type ReportHotspot = {
  rank: number;
  loss: number;
  count: number;
  lat: number;
  lon: number;
  avgLoss: number;
  maxDepthFt: number;
  windPct: number;
  waterPct: number;
  severity: { severe: number; major: number; moderate: number; minor: number; none: number };
  recommend: AdjusterRecommendation;
  routing: RoutingTag;
  // ── Phase 5: hazard mechanism breakdown ──────────────────
  // Aggregated from per-building hazard_mechanism field.
  // Optional so existing callers don't break.
  hazardMix?: HazardMix;
  // Aggregated from per-building loss_mechanism field (classify_loss_mechanism).
  // Keys: "surge_nfip" | "flood_nfip" | "compound_nfip" |
  //       "pluvial_homeowners" | "wind_homeowners" | "minimal"
  lossMechanismCounts?: Partial<Record<string, number>>;
};

export type ReportStorm = {
  name: string;
  year?: number;
  category: number;
  storm_id: string;
  max_wind_kt: number;
  min_pressure_mb?: number;
  landfall_lat?: number;
  landfall_lon?: number;
  population?: { county_name?: string; state_code?: string; population?: number; pop_label?: string };
};

export type ReportTotals = { buildings: number; loss: number; totalDepth: number };
export type ReportConfidence = { level: string; count: number };

// HTML-escape helper for any caller-provided strings.
const esc = (s: unknown): string =>
  String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const fmtUSD = (n: number): string => {
  if (!isFinite(n) || n <= 0) return '—';
  if (n >= 1e9) return `~$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `~$${Math.round(n / 1e6)}M`;
  if (n >= 1e3) return `~$${Math.round(n / 1e3)}K`;
  return `$${Math.round(n).toLocaleString()}`;
};

const fmtCount = (n: number): string => {
  if (!isFinite(n) || n <= 0) return '0';
  if (n >= 10_000) return `~${(n / 1000).toFixed(1)}k`;
  if (n >= 1_000) return `~${Math.round(n / 100) * 100}`;
  if (n >= 100) return `~${Math.round(n / 10) * 10}`;
  return Math.round(n).toLocaleString();
};

const SHARED_STYLES = `
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 820px; margin: 32px auto; padding: 0 24px; color: #0f172a; line-height: 1.5; }
  h1 { color: #0f172a; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin-bottom: 4px; font-size: 26px; }
  h1.em { border-bottom-color: #10b981; }
  h2 { color: #1e293b; border-left: 4px solid #f97316; padding-left: 10px; margin-top: 28px; font-size: 17px; }
  h2.em { border-left-color: #10b981; }
  .meta { color: #64748b; font-size: 13px; margin-top: 0; }
  table { width: 100%; border-collapse: collapse; margin: 8px 0 18px; font-size: 13px; }
  td { padding: 6px 10px; border-bottom: 1px solid #e2e8f0; }
  td:first-child { color: #64748b; width: 38%; }
  td:last-child { font-weight: 600; color: #0f172a; }
  .urgency { display: inline-block; padding: 4px 12px; border-radius: 4px; font-weight: 700; font-size: 13px; color: #fff; letter-spacing: 0.5px; text-transform: uppercase; }
  .urgency-immediate { background: #dc2626; }
  .urgency-cat       { background: #f97316; }
  .urgency-field     { background: #f59e0b; }
  .urgency-standard  { background: #0ea5e9; }
  .urgency-monitor   { background: #64748b; }
  .peril-bar { display: flex; height: 20px; border-radius: 10px; overflow: hidden; margin: 6px 0; border: 1px solid #cbd5e1; }
  .peril-water { background: #4f46e5; }
  .peril-wind  { background: #0ea5e9; }
  .area-card  { border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px 16px; margin-bottom: 12px; background: #f8fafc; page-break-inside: avoid; }
  .area-card h3 { margin: 0 0 4px; font-size: 15px; color: #0f172a; }
  .area-meta { font-size: 12px; color: #64748b; margin-bottom: 8px; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; margin-right: 4px; }
  .tag-nfip { background: #e0e7ff; color: #3730a3; }
  .tag-ho3  { background: #e0f2fe; color: #075985; }
  .tag-mixed { background: #ede9fe; color: #5b21b6; }
  .tag-covered { background: #dcfce7; color: #166534; }
  .tag-partial { background: #fef3c7; color: #92400e; }
  .tag-uncovered { background: #fecaca; color: #991b1b; }
  .confidence-note { background: #fefce8; border: 1px solid #fde68a; border-radius: 6px; padding: 10px 14px; margin: 16px 0; font-size: 12px; color: #713f12; }
  .disclaimer { background: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; padding: 12px 16px; margin-top: 28px; font-size: 12px; color: #7f1d1d; }
  .disclaimer-top { background: #fee2e2; border: 2px solid #dc2626; border-radius: 8px; padding: 14px 18px; margin: 12px 0 20px; font-size: 13px; color: #7f1d1d; }
  .disclaimer-top strong.banner { display: block; font-size: 15px; letter-spacing: 0.5px; color: #991b1b; margin-bottom: 4px; text-transform: uppercase; }
  .about { background: #f1f5f9; border-left: 4px solid #64748b; border-radius: 4px; padding: 10px 14px; margin: 0 0 20px; font-size: 12px; color: #334155; line-height: 1.55; }
  .about strong { color: #0f172a; }
  .footer { margin-top: 28px; padding-top: 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #94a3b8; text-align: center; }
  @media print {
    .disclaimer-top { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  }
  ul.contacts { padding-left: 18px; font-size: 13px; line-height: 1.7; }
  @media print { body { margin: 0; padding: 16px; } .no-print { display: none; } }
`;

// Prominent page-1 disclaimer banner, shared across both report types.
// Legal exposure guard — every export gets this at the top, not just the
// bottom, so non-technical readers can't miss it. (CAT_TEAM_PLAN §9)
const DISCLAIMER_TOP = `
<div class="disclaimer-top">
  <strong class="banner">⚠ Modeled estimate — not field verified</strong>
  This document is an automated forecast based on SLOSH storm-surge modeling
  and building-inventory estimates. It is intended to <strong>support</strong> —
  not replace — field inspection, direct communication with affected
  residents, and the judgment of professional CAT managers and emergency
  management authorities. Numbers are rounded, conservative, and should be
  treated as a starting point for planning conversations, not a binding
  deployment or evacuation order.
</div>`;

// Friendly "About this report" blurb — CAT_TEAM_PLAN §9 calls out SurgeDPS
// as an educational resource for students curious about disaster response.
// Sits below the disclaimer, above the first content heading.
const ABOUT_BLURB = `
<div class="about">
  <strong>About this report.</strong> SurgeDPS combines historical storm
  tracks, SLOSH surge modeling, and public building footprints to help
  insurance CAT teams and emergency managers ask better questions before
  and after a storm. If you're a student exploring disaster response as a
  career path, think of this document as a starting point for the
  conversations the pros actually have — not the final word on any
  particular storm.
</div>`;

const urgencyClass = (headline: string): string => {
  if (headline === 'Deploy immediately') return 'urgency-immediate';
  if (headline === 'Deploy CAT team')    return 'urgency-cat';
  if (headline === 'Deploy field adjusters') return 'urgency-field';
  if (headline === 'Standard claims handling') return 'urgency-standard';
  return 'urgency-monitor';
};

// ───────────────────────────────────────────────────────────
// §4b C4 — CAT Deployment Report
// ───────────────────────────────────────────────────────────
export function buildCatDeploymentReport(args: {
  storm: ReportStorm;
  totals: ReportTotals;
  severityCounts: Record<string, number>;
  hotspots: ReportHotspot[];
  estimatedPop: number;
  confidence: ReportConfidence;
  teamSize: number;
  windowDays: number;
}): string {
  const { storm, totals, severityCounts, hotspots, estimatedPop, confidence, teamSize, windowDays } = args;
  const now = new Date();
  const wl = workloadSummary(severityCounts);
  const stormMix = aggregatePerilMix(hotspots.map(h => ({ windPct: h.windPct, waterPct: h.waterPct, weight: h.count })));
  const plan = planDeployment(hotspots, teamSize, windowDays);
  const ttc = timeToClearDays(hotspots, teamSize);

  const refId = `CAT-${storm.storm_id}-${now.toISOString().slice(0,10).replace(/-/g,'')}`;

  const areasHtml = plan.areas.map((pa, i) => {
    const h = hotspots[i];
    if (!h) return '';
    const statusTag = pa.status === 'covered' ? 'tag-covered' : pa.status === 'partial' ? 'tag-partial' : 'tag-uncovered';
    const routingTag = h.routing.hint === 'nfip' ? 'tag-nfip' : h.routing.hint === 'ho3' ? 'tag-ho3' : 'tag-mixed';

    // Hazard mechanism breakdown (Phase 5)
    const hazardLine = h.hazardMix
      ? `<div style="font-size:12px;color:#475569;margin-top:3px">
           Hazard: <strong>${esc(hazardMechanismLabel(h.hazardMix))}</strong>
         </div>`
      : '';

    // NFIP vs homeowners split
    let routingSplitLine = '';
    if (h.lossMechanismCounts) {
      const split: LossRoutingSplit = lossRoutingSplit(h.lossMechanismCounts);
      routingSplitLine = `<div style="font-size:12px;color:#475569;margin-top:3px">
        Insurance routing: <strong style="color:#3730a3">NFIP ${split.nfipPct}%</strong>
        · <strong style="color:#075985">Homeowners ${split.homeownersPct}%</strong>
        ${split.otherPct > 0 ? `· Other ${split.otherPct}%` : ''}
      </div>`;
    }

    return `
    <div class="area-card">
      <h3>#${h.rank} — ${fmtUSD(h.loss)} modeled loss across ${fmtCount(h.count)} buildings</h3>
      <div class="area-meta">
        ~${h.lat.toFixed(3)}°N, ${Math.abs(h.lon).toFixed(3)}°W · max surge ~${Math.round(h.maxDepthFt)} ft · avg loss ${fmtUSD(h.avgLoss)}
      </div>
      <div>
        <span class="tag ${routingTag}">${esc(h.routing.label)}</span>
        <span class="tag ${statusTag}">${pa.status.toUpperCase()} (${pa.coverage_pct}%)</span>
      </div>
      <div class="peril-bar" title="${h.waterPct}% water / ${h.windPct}% wind">
        <div class="peril-water" style="width:${h.waterPct}%"></div>
        <div class="peril-wind"  style="width:${h.windPct}%"></div>
      </div>
      <div style="font-size:12px;color:#475569">
        Water ${h.waterPct}% · Wind ${h.windPct}% ·
        recommended: <strong>${esc(h.recommend.label)}</strong>
        (~${pa.required_days.toFixed(1)} adjuster-days)
      </div>
      ${hazardLine}
      ${routingSplitLine}
      <div style="font-size:12px;color:#475569;margin-top:4px">
        Severity: ${h.severity.severe} severe · ${h.severity.major} major · ${h.severity.moderate} moderate · ${h.severity.minor} minor
      </div>
    </div>`;
  }).join('');

  const locLabel = storm.population?.county_name && storm.population?.state_code
    ? `${storm.population.county_name}, ${storm.population.state_code}`
    : '—';

  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>CAT Deployment Report — ${esc(storm.name)} (${esc(storm.year ?? '')})</title>
<style>${SHARED_STYLES}</style></head><body>
<h1>CAT Deployment Report</h1>
<p class="meta">
  <strong>${esc(storm.name)} (${esc(storm.year ?? '')})</strong> — Category ${storm.category} |
  Generated ${now.toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' })} at ${now.toLocaleTimeString('en-US', { hour:'2-digit', minute:'2-digit' })} |
  Reference: <strong>${esc(refId)}</strong>
</p>

${DISCLAIMER_TOP}
${ABOUT_BLURB}

<h2>Deployment Recommendation</h2>
<p><span class="urgency ${urgencyClass(wl.headline)}">${esc(wl.headline)}</span></p>
<table>
  <tr><td>Storm</td><td>${esc(storm.name)} (${esc(storm.year ?? '')}) — Cat ${storm.category}</td></tr>
  <tr><td>Max winds</td><td>${Math.round(storm.max_wind_kt * 1.15078)} mph (${storm.max_wind_kt} kt)</td></tr>
  <tr><td>Landfall area</td><td>${esc(locLabel)}</td></tr>
  <tr><td>Residents in surge zone</td><td>~${fmtCount(estimatedPop)}</td></tr>
  <tr><td>Peril profile</td><td>${esc(perilHeadline(stormMix))}</td></tr>
  <tr><td>Workload</td><td>${esc(wl.summary)}</td></tr>
  <tr><td>Modeled loss (rounded)</td><td>${fmtUSD(totals.loss)}</td></tr>
  <tr><td>Buildings analyzed</td><td>${fmtCount(totals.buildings)}</td></tr>
</table>

<h2>Peril Mix — Storm-Wide</h2>
<div class="peril-bar">
  <div class="peril-water" style="width:${stormMix.waterPct}%"></div>
  <div class="peril-wind"  style="width:${stormMix.windPct}%"></div>
</div>
<div style="font-size:13px">
  <strong style="color:#4f46e5">Water ${stormMix.waterPct}%</strong> ·
  <strong style="color:#0ea5e9">Wind ${stormMix.windPct}%</strong>
  (weighted by building count across the top ${hotspots.length} hotspots)
</div>

<h2>Planned Deployment</h2>
<table>
  <tr><td>Planned team size</td><td>${teamSize} adjuster${teamSize === 1 ? '' : 's'}</td></tr>
  <tr><td>Window</td><td>${windowDays} day${windowDays === 1 ? '' : 's'}</td></tr>
  <tr><td>Capacity</td><td>${plan.capacity_adjuster_days.toFixed(0)} adjuster-days</td></tr>
  <tr><td>Required to clear</td><td>${plan.required_adjuster_days.toFixed(0)} adjuster-days</td></tr>
  <tr><td>Coverage</td><td>${plan.coverage_pct}% of top areas fully covered (${plan.utilization_pct}% capacity utilization)</td></tr>
  <tr><td>Storm-wide time to clear</td><td>${formatTimeToClear(ttc)} at this team size</td></tr>
  ${plan.shortfall_days > 0
    ? `<tr><td>Shortfall</td><td style="color:#b91c1c"><strong>${plan.shortfall_days.toFixed(0)} adjuster-days</strong> not covered within window</td></tr>`
    : ''}
</table>

<h2>Top Priority Areas</h2>
${areasHtml || '<p style="color:#64748b">No hotspots identified.</p>'}

<h2>Data Confidence</h2>
<div class="confidence-note">
  Building inventory confidence: <strong>${esc(confidence.level)}</strong>
  across ${fmtCount(confidence.count)} analyzed buildings.
  Surge depths reflect SLOSH maximum-of-maximums modeling —
  actual depths may have been lower if landfall did not coincide
  with local high tide.
</div>

<h2>Claims Routing Quick Reference</h2>
<ul class="contacts">
  <li><strong>NFIP-primary areas</strong> — route to flood adjusters first. Contact your NFIP Direct servicer or WYO partner.</li>
  <li><strong>HO3-primary areas</strong> — route to standard property adjusters. Wind-driven losses.</li>
  <li><strong>Mixed / dual-route areas</strong> — coordinate both carriers. Flag for complex-claim queue.</li>
</ul>

<div class="disclaimer">
  <strong>MODELED ESTIMATE — NOT FIELD VERIFIED.</strong>
  This report is intended to support — not replace — field inspection and
  professional CAT management judgment. Adjuster-day recommendations are
  based on conservative per-severity throughput assumptions and should be
  treated as a starting point, not a binding deployment plan.
</div>

<div class="footer">
  Generated by SurgeDPS (stormdps.com/surgedps) — ${now.toISOString()}<br>
  Reference: ${esc(refId)}
</div>
</body></html>`;
}

// ───────────────────────────────────────────────────────────
// §4c E4 — Public advisory draft (plain-text template)
//
// Intentionally template-filled rather than LLM-generated so the
// output is predictable and the EM can sanity-check every field.
// Exported so the Resource Staging panel can show it inline.
// ───────────────────────────────────────────────────────────
export function draftPublicAdvisory(args: {
  storm: ReportStorm;
  hotspots: ReportHotspot[];
  estimatedPop: number;
  criticalBreakdown: Array<{ icon: string; label: string; count: number }>;
  shelterBedsNeeded?: number;
  rescueTeams?: number;
}): string {
  const { storm, hotspots, estimatedPop, criticalBreakdown, shelterBedsNeeded, rescueTeams } = args;
  const maxDepth = hotspots.length > 0 ? Math.max(...hotspots.map(h => h.maxDepthFt), 0) : 0;
  const posture = worstShelterPosture(hotspots.map(h => h.maxDepthFt));
  const area = storm.population?.county_name || 'the affected area';
  const critList = criticalBreakdown.filter(c => c.count > 0);

  const action =
    posture.level === 'evacuate' ? 'EVACUATE IMMEDIATELY.'
    : posture.level === 'shelter-upper' ? 'Move to upper floors and shelter in place.'
    : 'Shelter in place until further notice.';

  const lines: string[] = [];
  lines.push(`⚠️ ${storm.name.toUpperCase()} (Cat ${storm.category}) — SITUATION UPDATE`);
  lines.push(``);
  lines.push(`Residents in surge zones across ${area}: ${action}`);
  if (maxDepth > 0) {
    lines.push(`Up to ~${Math.round(maxDepth)} ft of storm surge expected in the hardest-hit zones.`);
  }
  lines.push(`Approximately ${fmtCount(estimatedPop)} residents in potential surge zones.`);
  if (critList.length > 0) {
    const critText = critList.map(c => `${c.count} ${c.label.toLowerCase()}`).join(', ');
    lines.push(`Critical facilities impacted: ${critText}.`);
  } else {
    lines.push(`No critical facilities confirmed impacted at this time.`);
  }
  if (shelterBedsNeeded && shelterBedsNeeded > 0) {
    lines.push(`Sheltering capacity of ~${shelterBedsNeeded.toLocaleString()} beds being coordinated with partner agencies.`);
  }
  if (rescueTeams && rescueTeams > 0) {
    lines.push(`Mutual-aid request in progress: ${rescueTeams} swift-water / US&R team${rescueTeams === 1 ? '' : 's'}.`);
  }
  lines.push(``);
  lines.push(`If you are in a surge zone and unable to evacuate, move to the highest floor, stay away from windows, and call 911 only for life-threatening emergencies.`);
  lines.push(`Next update in 6 hours.`);

  return lines.join('\n');
}

// Compact one-line summary for the Resource Staging header.
export function posturePillLabel(maxDepthFt: number): string {
  return shelterPosture(maxDepthFt).label;
}

// ───────────────────────────────────────────────────────────
// §4a B8 — Situation Report (Emergency Manager focus)
// ───────────────────────────────────────────────────────────
export function buildSitRep(args: {
  storm: ReportStorm;
  totals: ReportTotals;
  severityCounts: Record<string, number>;
  hotspots: ReportHotspot[];
  estimatedPop: number;
  confidence: ReportConfidence;
  criticalBreakdown: Array<{ icon: string; label: string; count: number }>;
}): string {
  const { storm, totals, severityCounts, hotspots, estimatedPop, confidence, criticalBreakdown } = args;
  const now = new Date();
  const wl = workloadSummary(severityCounts);
  const stormMix = aggregatePerilMix(hotspots.map(h => ({ windPct: h.windPct, waterPct: h.waterPct, weight: h.count })));
  const critList = criticalBreakdown.filter(c => c.count > 0);
  const refId = `SITREP-${storm.storm_id}-${now.toISOString().slice(0,10).replace(/-/g,'')}`;

  const areasHtml = hotspots.map(h => {
    const evac = h.maxDepthFt >= 6 ? 'EVACUATE'
      : h.maxDepthFt >= 3 ? 'SHELTER UPPER FLOORS'
      : 'SHELTER IN PLACE';
    const evacColor = evac === 'EVACUATE' ? '#dc2626' : evac === 'SHELTER UPPER FLOORS' ? '#f59e0b' : '#10b981';

    // Hazard mechanism line (Phase 5 — shows surge/pluvial/compound breakdown)
    const hazardLine = h.hazardMix
      ? `<div style="font-size:12px;color:#475569;margin-top:3px">
           Hazard: <strong>${esc(hazardMechanismLabel(h.hazardMix))}</strong>
         </div>`
      : '';

    // NFIP vs homeowners split for the SitRep (critical for resource coordination)
    let routingSplitLine = '';
    if (h.lossMechanismCounts) {
      const split: LossRoutingSplit = lossRoutingSplit(h.lossMechanismCounts);
      routingSplitLine = `<div style="font-size:12px;color:#475569;margin-top:3px">
        Claims routing: NFIP ${split.nfipPct}% · Homeowners ${split.homeownersPct}%
        ${split.otherPct > 0 ? `· Other ${split.otherPct}%` : ''}
      </div>`;
    }

    return `
    <div class="area-card">
      <h3>#${h.rank} — ${fmtCount(h.count)} buildings impacted</h3>
      <div class="area-meta">
        ~${h.lat.toFixed(3)}°N, ${Math.abs(h.lon).toFixed(3)}°W · max surge ~${Math.round(h.maxDepthFt)} ft
      </div>
      <div><strong style="color:${evacColor}">${evac}</strong></div>
      <div class="peril-bar">
        <div class="peril-water" style="width:${h.waterPct}%"></div>
        <div class="peril-wind"  style="width:${h.windPct}%"></div>
      </div>
      <div style="font-size:12px;color:#475569">
        Water ${h.waterPct}% · Wind ${h.windPct}% ·
        ${h.severity.severe + h.severity.major} likely uninhabitable
      </div>
      ${hazardLine}
      ${routingSplitLine}
    </div>`;
  }).join('');

  // Public advisory draft — §4c E4. Uses the shared helper so the
  // Resource Staging panel renders the same copy the report ships.
  const advisory = draftPublicAdvisory({
    storm,
    hotspots,
    estimatedPop,
    criticalBreakdown,
  });

  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Situation Report — ${esc(storm.name)} (${esc(storm.year ?? '')})</title>
<style>${SHARED_STYLES}</style></head><body>
<h1 class="em">Situation Report</h1>
<p class="meta">
  <strong>${esc(storm.name)} (${esc(storm.year ?? '')})</strong> — Category ${storm.category} |
  Generated ${now.toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' })} at ${now.toLocaleTimeString('en-US', { hour:'2-digit', minute:'2-digit' })} |
  Reference: <strong>${esc(refId)}</strong>
</p>

${DISCLAIMER_TOP}
${ABOUT_BLURB}

<h2 class="em">Situation Overview</h2>
<p><span class="urgency ${urgencyClass(wl.headline)}">${esc(wl.headline)}</span></p>
<table>
  <tr><td>Storm</td><td>${esc(storm.name)} (${esc(storm.year ?? '')}) — Cat ${storm.category}</td></tr>
  <tr><td>Max winds</td><td>${Math.round(storm.max_wind_kt * 1.15078)} mph</td></tr>
  <tr><td>Residents in surge zone</td><td>~${fmtCount(estimatedPop)}</td></tr>
  <tr><td>Peril profile</td><td>${esc(perilHeadline(stormMix))}</td></tr>
  <tr><td>Affected buildings</td><td>${fmtCount(totals.buildings)}</td></tr>
  <tr><td>Likely uninhabitable</td><td>~${fmtCount(wl.uninhabitable)}</td></tr>
</table>

<h2 class="em">Critical Facilities Impacted</h2>
${critList.length > 0 ? `
<table>
  ${critList.map(c => `<tr><td>${esc(c.icon)} ${esc(c.label)}</td><td>${c.count}</td></tr>`).join('')}
</table>` : '<p style="color:#64748b">No critical facilities identified in the surge zone.</p>'}

<h2 class="em">Priority Zones</h2>
${areasHtml || '<p style="color:#64748b">No hotspots identified.</p>'}

<h2 class="em">Suggested Public Advisory</h2>
<pre style="background:#f1f5f9;border:1px solid #cbd5e1;border-radius:6px;padding:12px;font-family:inherit;font-size:12px;white-space:pre-wrap;line-height:1.55">${esc(advisory)}</pre>
<p style="font-size:11px;color:#64748b;margin-top:-8px">
  Template only — review against your agency's current guidance before public release.
</p>

<h2 class="em">Data Confidence</h2>
<div class="confidence-note">
  Building inventory confidence: <strong>${esc(confidence.level)}</strong>.
  Surge depths reflect SLOSH maximum-of-maximums modeling —
  actual depths vary with tidal alignment.
</div>

<div class="disclaimer">
  <strong>MODELED ESTIMATE — NOT FIELD VERIFIED.</strong>
  This report is intended for planning and situational awareness only.
  Evacuation and shelter decisions must be coordinated with the
  responsible local emergency management authority.
</div>

<div class="footer">
  Generated by SurgeDPS (stormdps.com/surgedps) — ${now.toISOString()}<br>
  Reference: ${esc(refId)}
</div>
</body></html>`;
}
