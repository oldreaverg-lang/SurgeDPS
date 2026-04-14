// ─────────────────────────────────────────────────────────────────────────────
// useHazardLayers — hazard view mode (surge / rainfall / compound),
// plus the async fetches for MRMS rainfall stats, QPF, and compound tiles.
// ─────────────────────────────────────────────────────────────────────────────

import { useState } from 'react';
import type { RainfallStats, QpfStats, CompoundStats } from '../types';

export function useHazardLayers() {
  const [hazardView, setHazardView] = useState<'surge' | 'rainfall' | 'compound'>('surge');

  // Observed MRMS rainfall
  const [rainfallStats, setRainfallStats] = useState<RainfallStats | null>(null);
  const [rainfallLoading, setRainfallLoading] = useState(false);
  const [rainfallMode, setRainfallMode] = useState<'observed' | 'forecast'>('observed');

  // WPC QPF (forecast rainfall)
  const [qpfStats, setQpfStats] = useState<QpfStats | null>(null);
  const [qpfLoading, setQpfLoading] = useState(false);

  // Compound (surge ∪ rainfall ∪ fluvial)
  const [compoundStats, setCompoundStats] = useState<CompoundStats | null>(null);
  const [compoundLoading, setCompoundLoading] = useState(false);

  return {
    hazardView, setHazardView,
    rainfallStats, setRainfallStats,
    rainfallLoading, setRainfallLoading,
    rainfallMode, setRainfallMode,
    qpfStats, setQpfStats,
    qpfLoading, setQpfLoading,
    compoundStats, setCompoundStats,
    compoundLoading, setCompoundLoading,
  };
}
