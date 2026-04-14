// ─────────────────────────────────────────────────────────────────────────────
// Building type and critical facility helpers.
// ─────────────────────────────────────────────────────────────────────────────

export const BUILDING_TYPES: Record<string, string> = {
  RES1: 'Single-Family Home', RES2: 'Mobile Home', RES3: 'Multi-Family Housing',
  RES4: 'Hotel / Motel', RES5: 'Dormitory', RES6: 'Nursing Home',
  COM1: 'Retail Store', COM2: 'Warehouse', COM3: 'Service Business',
  COM4: 'Office Building', COM5: 'Bank / Financial', COM6: 'Hospital',
  COM7: 'Medical Clinic', COM8: 'Entertainment Venue', COM9: 'Theater',
  COM10: 'Parking Structure', IND1: 'Heavy Industrial', IND2: 'Light Industrial',
  IND3: 'Food / Chemical Plant', IND4: 'Metal / Minerals Facility',
  IND5: 'High-Tech Industrial', IND6: 'Construction Facility',
  AGR1: 'Agricultural Building', REL1: 'Church / Place of Worship',
  GOV1: 'Government Building', GOV2: 'Emergency Services',
  EDU1: 'School', EDU2: 'College / University',
};

/**
 * Icon map keyed on raw NSI `occtype` prefix. The backend collapses
 * GOV/EDU/REL → "COM" in `building_type` for depth-damage curves,
 * but preserves the civic distinction in `occtype`.
 */
export const CRITICAL_ICONS: Record<string, string> = {
  EDU1: '🏫', EDU2: '🏫',
  MED1: '➕', MED2: '➕',
  COM6: '➕', COM7: '➕',
  GOV1: '⭐', GOV2: '⭐',
  REL1: '⛪',
  RES6: '🛏️',
};

/** Prefer occtype (full detail) over building_type (HAZUS-collapsed). */
export function criticalPrefix(p: any): string {
  const raw = (p?.occtype || p?.building_type || '');
  return String(raw).replace(/[-_].*$/, '').toUpperCase();
}

export function friendlyBuildingType(code: string): string {
  if (!code) return 'Unknown';
  const prefix = code.replace(/[-_].*$/, '').toUpperCase();
  if (BUILDING_TYPES[prefix]) return BUILDING_TYPES[prefix];
  if (prefix === 'COM') return 'Commercial / Civic';
  if (prefix === 'IND') return 'Industrial';
  if (prefix === 'RES') return 'Residential';
  return code;
}

export function friendlyFacilityLabel(p: any): string {
  return friendlyBuildingType(p?.occtype || p?.building_type);
}
