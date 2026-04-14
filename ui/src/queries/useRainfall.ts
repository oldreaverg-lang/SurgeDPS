// ─────────────────────────────────────────────────────────────────────────────
// useRainfall / useQpf / useCompound — raster overlay metadata queries.
// These return stats + tile URL; the tile URL is fed into a raster Source.
// ─────────────────────────────────────────────────────────────────────────────

import { useQuery } from '@tanstack/react-query';
import type { RainfallStats, QpfStats, CompoundStats } from '../types';

// ── Observed rainfall ────────────────────────────────────────────────────────
export const rainfallKey = (stormId: string | null) =>
  ['rainfall', stormId] as const;

async function fetchRainfall(stormId: string): Promise<RainfallStats> {
  const res = await fetch(
    `/surgedps/api/rainfall_overlay?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`rainfall_overlay ${res.status}`);
  return res.json();
}

export function useRainfall(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: rainfallKey(stormId),
    queryFn: () => fetchRainfall(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 5 * 60 * 1000,
    gcTime:   20 * 60 * 1000,
    retry: 1,
  });
}

// ── QPF (quantitative precipitation forecast) ────────────────────────────────
export const qpfKey = (stormId: string | null) => ['qpf', stormId] as const;

async function fetchQpf(stormId: string): Promise<QpfStats> {
  const res = await fetch(
    `/surgedps/api/qpf_overlay?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`qpf_overlay ${res.status}`);
  return res.json();
}

export function useQpf(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: qpfKey(stormId),
    queryFn: () => fetchQpf(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 5 * 60 * 1000,
    gcTime:   20 * 60 * 1000,
    retry: 1,
  });
}

// ── Compound surge+rain mosaic ────────────────────────────────────────────────
export const compoundKey = (stormId: string | null) =>
  ['compound', stormId] as const;

async function fetchCompound(stormId: string): Promise<CompoundStats> {
  const res = await fetch(
    `/surgedps/api/compound_overlay?storm_id=${encodeURIComponent(stormId)}`,
  );
  if (!res.ok) throw new Error(`compound_overlay ${res.status}`);
  return res.json();
}

export function useCompound(stormId: string | null, enabled = false) {
  return useQuery({
    queryKey: compoundKey(stormId),
    queryFn: () => fetchCompound(stormId!),
    enabled: enabled && stormId != null,
    staleTime: 5 * 60 * 1000,
    gcTime:   20 * 60 * 1000,
    retry: 1,
  });
}
