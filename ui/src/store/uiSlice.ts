// ─────────────────────────────────────────────────────────────────────────────
// uiSlice — display mode, persona, sidebar, and operator-level toggles.
// ─────────────────────────────────────────────────────────────────────────────

import type { StateCreator } from 'zustand';
import type { DisplayMode } from '../types';
import type { SubPersona } from '../catTeam';

export interface UiSlice {
  // ── Mode / persona ──────────────────────────────────────────────────────────
  mode: DisplayMode;
  subPersona: SubPersona;

  // ── Sidebar / panels ────────────────────────────────────────────────────────
  sidebarOpen: boolean;
  moreMenuOpen: boolean;
  betaLayersEnabled: boolean;

  // ── Deployment planner ──────────────────────────────────────────────────────
  teamSize: number;
  windowDays: number;

  // ── Storm browser ───────────────────────────────────────────────────────────
  selectedYear: number | null;
  stormBrowserOpen: boolean;

  // ── Peril filter ────────────────────────────────────────────────────────────
  perilView: 'surge' | 'rainfall' | 'cumulative';

  // ── Map view ─────────────────────────────────────────────────────────────────
  mapView: 'buildings' | 'county' | 'city';

  // ── Actions ─────────────────────────────────────────────────────────────────
  setMode: (mode: DisplayMode) => void;
  setSubPersona: (p: SubPersona) => void;
  setSidebarOpen: (v: boolean) => void;
  toggleSidebar: () => void;
  setMoreMenuOpen: (v: boolean) => void;
  setBetaLayersEnabled: (v: boolean) => void;
  setTeamSize: (n: number) => void;
  setWindowDays: (n: number) => void;
  setSelectedYear: (y: number | null) => void;
  setStormBrowserOpen: (v: boolean) => void;
  setPerilView: (p: UiSlice['perilView']) => void;
  setMapView: (v: UiSlice['mapView']) => void;
}

export const createUiSlice: StateCreator<UiSlice, [], [], UiSlice> = (set) => ({
  mode: 'analyst',
  subPersona: 'cat',
  sidebarOpen: false,
  moreMenuOpen: false,
  betaLayersEnabled: false,
  teamSize: 10,
  windowDays: 7,
  selectedYear: null,
  stormBrowserOpen: false,
  perilView: 'surge',
  mapView: 'buildings',

  setMode: (mode) => set({ mode }),
  setSubPersona: (p) => set({ subPersona: p }),
  setSidebarOpen: (v) => set({ sidebarOpen: v }),
  toggleSidebar: () => set((s) => ({ sidebarOpen: !s.sidebarOpen })),
  setMoreMenuOpen: (v) => set({ moreMenuOpen: v }),
  setBetaLayersEnabled: (v) => set({ betaLayersEnabled: v }),
  setTeamSize: (n) => set({ teamSize: n }),
  setWindowDays: (n) => set({ windowDays: n }),
  setSelectedYear: (y) => set({ selectedYear: y }),
  setStormBrowserOpen: (v) => set({ stormBrowserOpen: v }),
  setPerilView: (p) => set({ perilView: p }),
  setMapView: (v) => set({ mapView: v }),
});
