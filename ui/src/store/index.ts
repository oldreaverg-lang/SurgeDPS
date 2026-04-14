// ─────────────────────────────────────────────────────────────────────────────
// SurgeDPS store — combines storm, ui, and map slices into a single Zustand
// store. Use slice selectors to avoid unnecessary re-renders.
//
// Usage:
//   import { useSurgeStore } from '../store';
//   const activeStorm = useSurgeStore(s => s.activeStorm);
//   const setMode = useSurgeStore(s => s.setMode);
// ─────────────────────────────────────────────────────────────────────────────

import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { createStormSlice } from './stormSlice';
import { createUiSlice } from './uiSlice';
import { createMapSlice } from './mapSlice';
import type { StormSlice } from './stormSlice';
import type { UiSlice } from './uiSlice';
import type { MapSlice } from './mapSlice';

export type SurgeStore = StormSlice & UiSlice & MapSlice;

export const useSurgeStore = create<SurgeStore>()(
  devtools(
    (...args) => ({
      ...createStormSlice(...args),
      ...createUiSlice(...args),
      ...createMapSlice(...args),
    }),
    { name: 'SurgeDPS' },
  ),
);

// ── Convenience selector hooks ───────────────────────────────────────────────
// These narrow the subscription to a single slice so a component that only
// cares about UI state doesn't re-render when building GeoJSON changes.

export const useStormStore = <T>(sel: (s: StormSlice) => T): T =>
  useSurgeStore(sel as (s: SurgeStore) => T);

export const useUiStore = <T>(sel: (s: UiSlice) => T): T =>
  useSurgeStore(sel as (s: SurgeStore) => T);

export const useMapStore = <T>(sel: (s: MapSlice) => T): T =>
  useSurgeStore(sel as (s: SurgeStore) => T);

// Re-export slice types for consumers
export type { StormSlice, UiSlice, MapSlice };
export type { BasemapKey, ViewportBounds } from './mapSlice';
export type { LoadedCell } from './stormSlice';
