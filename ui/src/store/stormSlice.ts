// ─────────────────────────────────────────────────────────────────────────────
// stormSlice — active storm + building data.
//
// Wraps the state that was previously scattered across App.tsx useState calls:
// activeStorm, activating, allBuildings, displayBuildings, loadedCells, etc.
// ─────────────────────────────────────────────────────────────────────────────

import type { StateCreator } from 'zustand';
import type { StormInfo } from '../types';

export interface LoadedCell {
  col: number;
  row: number;
  /** 'loading' | 'done' | 'error' */
  status: string;
}

export interface StormSlice {
  // ── Active storm ────────────────────────────────────────────────────────────
  activeStorm: StormInfo | null;
  activating: boolean;
  activateError: string | null;

  // ── Cell loading ────────────────────────────────────────────────────────────
  /** Map of cellKey → LoadedCell */
  loadedCells: Record<string, LoadedCell>;
  loadingCells: Set<string>;

  // ── Building GeoJSON ────────────────────────────────────────────────────────
  allBuildings: any | null;
  displayBuildings: any | null;

  // ── DPS / confidence ────────────────────────────────────────────────────────
  dpsScore: number | null;
  confidenceLevel: string | null;
  eliValue: number | null;

  // ── Progress (activation polling) ──────────────────────────────────────────
  progress: { step: string; step_num: number; total_steps: number; elapsed: number } | null;

  // ── Actions ─────────────────────────────────────────────────────────────────
  setActiveStorm: (storm: StormInfo | null) => void;
  setActivating: (v: boolean) => void;
  setActivateError: (msg: string | null) => void;
  setLoadedCells: (cells: Record<string, LoadedCell>) => void;
  addLoadedCell: (key: string, cell: LoadedCell) => void;
  setLoadingCells: (cells: Set<string>) => void;
  addLoadingCell: (key: string) => void;
  removeLoadingCell: (key: string) => void;
  setAllBuildings: (data: any | null) => void;
  setDisplayBuildings: (data: any | null) => void;
  setDpsScore: (v: number | null) => void;
  setConfidenceLevel: (v: string | null) => void;
  setEliValue: (v: number | null) => void;
  setProgress: (p: StormSlice['progress']) => void;
  resetStorm: () => void;
}

const initialStormState = {
  activeStorm: null,
  activating: false,
  activateError: null,
  loadedCells: {},
  loadingCells: new Set<string>(),
  allBuildings: null,
  displayBuildings: null,
  dpsScore: null,
  confidenceLevel: null,
  eliValue: null,
  progress: null,
};

export const createStormSlice: StateCreator<StormSlice, [], [], StormSlice> = (set) => ({
  ...initialStormState,

  setActiveStorm: (storm) => set({ activeStorm: storm }),
  setActivating: (v) => set({ activating: v }),
  setActivateError: (msg) => set({ activateError: msg }),

  setLoadedCells: (cells) => set({ loadedCells: cells }),
  addLoadedCell: (key, cell) =>
    set((s) => ({ loadedCells: { ...s.loadedCells, [key]: cell } })),

  setLoadingCells: (cells) => set({ loadingCells: cells }),
  addLoadingCell: (key) =>
    set((s) => {
      const next = new Set(s.loadingCells);
      next.add(key);
      return { loadingCells: next };
    }),
  removeLoadingCell: (key) =>
    set((s) => {
      const next = new Set(s.loadingCells);
      next.delete(key);
      return { loadingCells: next };
    }),

  setAllBuildings: (data) => set({ allBuildings: data }),
  setDisplayBuildings: (data) => set({ displayBuildings: data }),

  setDpsScore: (v) => set({ dpsScore: v }),
  setConfidenceLevel: (v) => set({ confidenceLevel: v }),
  setEliValue: (v) => set({ eliValue: v }),
  setProgress: (p) => set({ progress: p }),

  resetStorm: () => set({ ...initialStormState, loadingCells: new Set<string>() }),
});
