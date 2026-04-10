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
} from './catTeam';
import type { RoutingTag, AdjusterRecommendation } from './catTeam';

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
  .footer { margin-top: 28px; padding-top: 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #94a3b8; text-align: center; }
  ul.contacts { padding-left: 18px; font-size: 13px; line-height: 1.7; }
  @media print { body { margin: 0; padding: 16px; } .no-print { display: none; } }
`;

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
      <div style="font-size:12px;color:#475569;margin-top:4px">
        Severity mix:
        ${h.severity.severe} severe ·
        ${h.severity.major} major ·
        ${h.severity.moderate} moderate ·
        ${h.severity.minor} minor
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
    </div>`;
  }).join('');

  // Simple public advisory template (§4c E4 stub — Phase 4 will
  // upgrade this to something the EM can actually post verbatim).
  const advisory = `⚠️ ${storm.name.toUpperCase()} (Cat ${storm.category}) — SITUATION UPDATE
Residents in surge zones across ${storm.population?.county_name || 'the affected area'}: ${
  hotspots.some(h => h.maxDepthFt >= 6) ? 'EVACUATE IMMEDIATELY.' : 'shelter in place until further notice.'
}
Up to ${Math.round(Math.max(...hotspots.map(h => h.maxDepthFt), 0))} ft of storm surge expected.
Approximately ${fmtCount(estimatedPop)} residents in potential surge zones.
Critical facilities impacted: ${critList.map(c => `${c.count} ${c.label.toLowerCase()}`).join(', ') || 'none reported'}.
Next update in 6 hours.`;

  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Situation Report — ${esc(storm.name)} (${esc(storm.year ?? '')})</title>
<style>${SHARED_STYLES}</style></head><body>
<h1 class="em">Situation Report</h1>
<p class="meta">
  <strong>${esc(storm.name)} (${esc(storm.year ?? '')})</strong> — Category ${storm.category} |
  Generated ${now.toLocaleDateString('en-US', { year:'numeric', month:'long', day:'numeric' })} at ${now.toLocaleTimeString('en-US', { hour:'2-digit', minute:'2-digit' })} |
  Reference: <strong>${esc(refId)}</strong>
</p>

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
