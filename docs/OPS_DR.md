# SurgeDPS — Disaster recovery runbook

## What's on the volume vs. how to get it back

Railway's persistent volume holds everything the warm-cache phases
produce. If the volume is lost — bad migration, accidental detach,
provider incident — here's what survives in git and what we have to
rebuild.

| Path | Size | In git? | How to rebuild |
|---|---|---|---|
| `cells/<storm>/` | ~7 GB | No | warm_cache Phase 1 (~30 min for 33 sidebar storms) |
| `mrms/` | ~700 MB | No | warm_cache Phase 5 (~25 min for IEM-era storms; pre-IEM storms can't be rebuilt) |
| `cache/gauges_historical/` | ~8 MB | No | warm_cache Phase 2 (~15 min) |
| **`cache/flood_zones/`** | **~4 GB** | **No** | **Cannot rebuild from Railway** — FEMA WAF blocks the Railway egress IP. Requires running `scripts/seed_flood_zones_local.py` from a dev machine (hours). |
| `data/validation/` | ~92 KB | Yes | git pull |
| `monitor_state.json` | <1 KB | No | Self-rebuilds on next NHC poll |
| `data/dps_scores.json` | small | Yes | git pull |

**The one segment we can't recreate by re-running phases is `cache/flood_zones/`.** Everything else is regenerable; flood_zones is the priority for snapshot backups.

---

## Snapshot endpoint — `/__val/backup/<segment>`

Token-gated, streaming tar.gz of one volume segment. Segments:

- `flood_zones` — the irreplaceable one
- `gauges_historical` — easy to rebuild, but a backup means restore is seconds vs ~15 min
- `mrms` — same trade-off; bigger archive

```bash
TOKEN=<VALIDATION_TOKEN from Railway service vars>
BASE=https://surgedps-production.up.railway.app

# Required: flood zones (the one that can't be re-fetched from Railway)
curl -sS -o "surgedps_backup_flood_zones_$(date -u +%Y%m%dT%H%M%SZ).tar.gz" \
  "$BASE/__val/backup/flood_zones?t=$TOKEN"

# Optional: gauges + MRMS for fast-restore
curl -sS -o "surgedps_backup_gauges_$(date -u +%Y%m%dT%H%M%SZ).tar.gz" \
  "$BASE/__val/backup/gauges_historical?t=$TOKEN"
curl -sS -o "surgedps_backup_mrms_$(date -u +%Y%m%dT%H%M%SZ).tar.gz" \
  "$BASE/__val/backup/mrms?t=$TOKEN"
```

Total nightly transfer: ~4-5 GB. Push to S3 / Backblaze / wherever:

```bash
aws s3 cp "surgedps_backup_flood_zones_*.tar.gz" \
  "s3://my-backups/surgedps/flood_zones/" \
  --storage-class STANDARD_IA
```

### Recommended cadence

- **flood_zones**: nightly (changes only when you re-run the local seed script)
- **gauges_historical**: weekly (changes when warm_cache Phase 2 picks up a new storm)
- **mrms**: weekly (same — new storms only)

Keep 14 days of nightlies + 12 weeks of weeklies. Cost is a few GB-months at most.

---

## Restore

There's deliberately no upload endpoint — the seed script is the right
restore path for flood_zones, and the other segments are fast enough to
rebuild that a manual restore isn't worth the risk of a corrupt write.

### flood_zones (the hard case)

1. Confirm the loss: `curl "$BASE/__val/inventory.json?t=$TOKEN" | grep flood_zones_mb` → should be 0 or very low.
2. Untar the latest backup locally:
   ```bash
   tar -xzf surgedps_backup_flood_zones_*.tar.gz -C ./restore/
   # Produces ./restore/flood_zones/fz_*.json[.gz]
   ```
3. Re-upload each tile via the existing seed endpoint. The seed script
   is built for this — point it at the restore dir:
   ```bash
   cd SurgeDPS
   python scripts/seed_flood_zones_local.py \
     --restore-from ../restore/flood_zones \
     --token $TOKEN
   ```
   (If `--restore-from` isn't implemented yet, the lower-overhead path
   is to upload each file with a one-liner; the seed endpoint accepts
   gzipped JSON tile payloads.)

### gauges_historical / mrms (the easy cases)

Just let warm_cache rebuild on the next deploy. Or, if you want to skip
the wait, untar the backup into the live volume manually via Railway's
file browser (slow) or just trigger warm phases manually:

```bash
# Touch any push to main to re-trigger Phase 2 + Phase 5
git commit --allow-empty -m "Re-run warm phases after volume restore"
git push
```

---

## Disaster scenarios + RPO/RTO

| Scenario | Detection | Recovery time | Data loss |
|---|---|---|---|
| Container restart (deploy, OOM) | `/api/health` quick check | <2 min | None — volume persists |
| Volume detached but caches intact (Railway plan change) | `/api/health/storage` shows wrong root | <5 min | None |
| Volume corrupted, lost cells/ | `/api/health` warm_phase_complete=false; cells_mb=0 | ~30 min (re-run Phase 1) | None — fully regenerable |
| Volume corrupted, lost mrms/ | `mrms_mb` drops to 0 | ~25 min (Phase 5) | Pre-IEM storms (Katrina/Ike/Sandy) lose rainfall — no IEM archive that far back |
| **Volume corrupted, lost flood_zones/** | `flood_zones_mb` = 0; UI shows "no FEMA zones" | **Hours** without backup; minutes with backup | Without backup: full re-seed required from dev machine |
| Railway region down | All endpoints unreachable, dashboard reports down | Whatever Railway takes | None — volume in Railway's region survives |

The flood_zones row is the only one where having (or not having) a
nightly backup makes the difference between "minutes of inconvenience"
and "a Saturday of running the seed script."

---

## Verifying a backup works

Once a week, take a backup and untar it to /tmp to confirm it's not corrupt:

```bash
LATEST=$(ls -t surgedps_backup_flood_zones_*.tar.gz | head -1)
tar -tzf "$LATEST" | head -5  # Should list flood_zones/fz_*.json[.gz]
tar -tzf "$LATEST" | wc -l    # Should be ~3000-4000 entries
gzip -tv "$LATEST"            # gzip integrity check
```

If `gzip -tv` errors out, the backup got truncated mid-stream (probably
a client disconnect during download). Take another.
