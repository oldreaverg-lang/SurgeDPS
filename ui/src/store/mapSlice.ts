// ─────────────────────────────────────────────────────────────────────────────
// mapSlice — basemap, zoom level, viewport bounds, and overlay toggles.
// ─────────────────────────────────────────────────────────────────────────────

import type { StateCreator } from 'zustand';

export type BasemapKey = 'dark' | 'satellite' | 'street';

export interface ViewportBounds {
  west: number;
  south: number;
  east: number;
  north: number;
}

export interface MapSlice {
  // ── Basemap ─────────────────────────────────────────────────────────────────
  basemap: BasemapKey;

  // ── Viewport ────────────────────────────────────────────────────────────────
  zoom: number;
  bounds: ViewportBounds | null;

  // ── Sim / draw mode ─────────────────────────────────────────────────────────
  simActive: boolean;
  simMarker: { lng: number; lat: number } | null;

  // ── Popup ────────────────────────────────────────────────────────────────────
  popupInfo: {
    longitude: number;
    latitude: number;
    properties: Record<string, any>;
  } | null;

  // ── Actions ─────────────────────────────────────────────────────────────────
  setBasemap: (key: BasemapKey) => void;
  setZoom: (z: number) => void;
  setBounds: (b: ViewportBounds | null) => void;
  setSimActive: (v: boolean) => void;
  setSimMarker: (m: MapSlice['simMarker']) => void;
  setPopupInfo: (info: MapSlice['popupInfo']) => void;
  clearPopup: () => void;
}

export const createMapSlice: StateCreator<MapSlice, [], [], MapSlice> = (set) => ({
  basemap: 'dark',
  zoom: 6,
  bounds: null,
  simActive: false,
  simMarker: null,
  popupInfo: null,

  setBasemap: (key) => set({ basemap: key }),
  setZoom: (z) => set({ zoom: z }),
  setBounds: (b) => set({ bounds: b }),
  setSimActive: (v) => set({ simActive: v }),
  setSimMarker: (m) => set({ simMarker: m }),
  setPopupInfo: (info) => set({ popupInfo: info }),
  clearPopup: () => set({ popupInfo: null }),
});
