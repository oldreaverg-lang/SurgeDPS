// ─────────────────────────────────────────────────────────────────────────────
// useDisplayPreferences — analyst/ops mode, sub-persona, beta flag, team size.
// All persisted best-effort to localStorage.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from 'react';
import type { DisplayMode } from '../types';
import type { SubPersona } from '../catTeam';
import { readBetaLayersEnabled, writeBetaLayersEnabled } from '../betaLayers';

function lsGet(key: string): string | null {
  if (typeof window === 'undefined') return null;
  try { return window.localStorage.getItem(key); } catch { return null; }
}
function lsSet(key: string, value: string): void {
  try { window.localStorage.setItem(key, value); } catch { /* ignore */ }
}

export function useDisplayPreferences() {
  const [mode, setMode] = useState<DisplayMode>(() =>
    lsGet('surgedps.mode') === 'ops' ? 'ops' : 'analyst',
  );
  useEffect(() => { lsSet('surgedps.mode', mode); }, [mode]);

  const [subPersona, setSubPersona] = useState<SubPersona>(() =>
    lsGet('surgedps.subpersona') === 'em' ? 'em' : 'cat',
  );
  useEffect(() => { lsSet('surgedps.subpersona', subPersona); }, [subPersona]);

  const [betaLayersEnabled, setBetaLayersEnabled] = useState<boolean>(
    () => readBetaLayersEnabled(),
  );
  useEffect(() => { writeBetaLayersEnabled(betaLayersEnabled); }, [betaLayersEnabled]);

  const [teamSize, setTeamSize] = useState<number>(20);
  const [windowDays, setWindowDays] = useState<number>(5);

  return {
    mode, setMode,
    subPersona, setSubPersona,
    betaLayersEnabled, setBetaLayersEnabled,
    teamSize, setTeamSize,
    windowDays, setWindowDays,
  };
}
