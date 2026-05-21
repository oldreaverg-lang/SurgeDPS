// Lazy maplibre-gl loader. Returns a memoised Promise that resolves to the
// maplibregl module after the CSS + pmtiles protocol have been wired up.
//
// Why this exists:
//   App.tsx used to statically `import maplibregl from 'maplibre-gl'` plus
//   the CSS, which forced Vite to ship them in the eager bundle and made
//   the welcome screen pay maplibre's parse cost (~200 ms TBT) and its
//   CSS render-block (~410 ms) even when no storm was active. Per
//   Lighthouse's "Reduce unused JavaScript" finding, 231 KB of maplibre-gl
//   was downloaded but never executed on the welcome screen.
//
//   Passing this getter to <Map mapLib={...}/> defers the import to the
//   moment the Map component first mounts, matching what @vis.gl/react-
//   maplibre would have done by default if mapLib were omitted — but here
//   we also pre-wire the pmtiles protocol + load the CSS in the same
//   dynamic chunk so nothing has to retry on first interaction.

let _cached: Promise<any> | null = null;

export function getLazyMapLib() {
  if (_cached) return _cached;
  _cached = (async () => {
    // Run the three imports in parallel — maplibre is the largest payload,
    // pmtiles + the CSS chunk are small. Top-level await keeps the protocol
    // registration ordered after maplibre resolves.
    const [mlgMod, pmtilesMod] = await Promise.all([
      import('maplibre-gl'),
      import('pmtiles'),
      // Vite turns this into a dynamic CSS chunk that loads in parallel
      // with the JS. The CSS link is created when the import resolves,
      // not at HTML parse time, so it never blocks the welcome paint.
      import('maplibre-gl/dist/maplibre-gl.css'),
    ]);
    try {
      const protocol = new pmtilesMod.Protocol();
      mlgMod.default.addProtocol('pmtiles', protocol.tile);
    } catch {
      // pmtiles not installed or registration races a hot reload — non-fatal
    }
    return mlgMod.default;
  })();
  return _cached;
}
