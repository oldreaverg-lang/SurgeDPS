// ─────────────────────────────────────────────────────────────────────────────
// useAddressSearch — geocode address → fly map to coordinates.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useRef, useCallback, useEffect } from 'react';

export function useAddressSearch(
  mapRef: React.RefObject<any>,
  onPopupOpen?: (lon: number, lat: number) => void,
) {
  const [addressQuery, setAddressQuery] = useState('');
  const [addressSearching, setAddressSearching] = useState(false);
  const [addressError, setAddressError] = useState('');
  const flyToPopupTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (flyToPopupTimer.current) clearTimeout(flyToPopupTimer.current);
  }, []);

  const handleAddressSearch = useCallback(async () => {
    if (!addressQuery.trim()) return;
    setAddressSearching(true);
    setAddressError('');
    try {
      const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(addressQuery)}&limit=1`;
      const res = await fetch(url, { headers: { 'Accept-Language': 'en' } });
      const data = await res.json();
      if (!data?.length) { setAddressError('Address not found'); return; }
      const { lon, lat } = data[0];
      const lngNum = parseFloat(lon);
      const latNum = parseFloat(lat);
      mapRef.current?.flyTo({ center: [lngNum, latNum], zoom: 14, duration: 1500 });
      if (onPopupOpen) {
        flyToPopupTimer.current = setTimeout(() => onPopupOpen(lngNum, latNum), 1600);
      }
    } catch {
      setAddressError('Geocoding failed. Try again.');
    } finally {
      setAddressSearching(false);
    }
  }, [addressQuery, mapRef, onPopupOpen]);

  return {
    addressQuery, setAddressQuery,
    addressSearching,
    addressError,
    handleAddressSearch,
  };
}
