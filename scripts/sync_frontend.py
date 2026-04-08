#!/usr/bin/env python3
"""
sync_frontend.py — SurgeDPS → StormDPS frontend sync

Replaces the manual build-copy-edit workflow with a single command.

Usage (from SurgeDPS/ root):
    python scripts/sync_frontend.py           # build + sync
    python scripts/sync_frontend.py --dry-run # preview changes without writing

What it does:
    1. Runs `npm run build` inside SurgeDPS/ui/
    2. Discovers all new hashed asset filenames in dist/assets/
    3. Clears old hashed assets from StormDPS/frontend/surgedps/assets/
    4. Copies new assets across
    5. Updates the <script> and <link> tags in StormDPS/frontend/surgedps/index.html

Assumptions:
    - This script lives at SurgeDPS/scripts/sync_frontend.py
    - StormDPS repo is at ../StormDPS relative to SurgeDPS root
    - Vite outputs assets to ui/dist/assets/
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).resolve().parent
SURGE_ROOT   = SCRIPT_DIR.parent                          # SurgeDPS/
UI_DIR       = SURGE_ROOT / "ui"                          # SurgeDPS/ui/
DIST_ASSETS  = UI_DIR / "dist" / "assets"                 # SurgeDPS/ui/dist/assets/

STORM_ROOT   = SURGE_ROOT.parent / "StormDPS"             # ../StormDPS/
DEST_ASSETS  = STORM_ROOT / "frontend" / "surgedps" / "assets"
INDEX_HTML   = STORM_ROOT / "frontend" / "surgedps" / "index.html"


def run_build(dry_run: bool) -> None:
    if dry_run:
        print("[dry-run] Would run: npm run build  (inside ui/)")
        return
    print("── Building frontend ────────────────────────────────")
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=UI_DIR,
        check=True,
    )
    if result.returncode != 0:
        print("ERROR: npm build failed", file=sys.stderr)
        sys.exit(1)
    print("Build complete.\n")


def discover_assets(dry_run: bool) -> dict[str, Path]:
    """
    Return a dict mapping asset role → Path for each file in dist/assets/.
    Roles: 'js_main', 'css_main', and any extra chunks (e.g. maplibre).
    """
    if dry_run:
        print("[dry-run] Would scan dist/assets/ for hashed filenames")
        return {}

    if not DIST_ASSETS.exists():
        print(f"ERROR: dist/assets/ not found at {DIST_ASSETS}", file=sys.stderr)
        sys.exit(1)

    assets: dict[str, Path] = {}
    for f in DIST_ASSETS.iterdir():
        if f.suffix == ".js":
            # Distinguish main bundle from chunk files
            role = "js_main" if f.name.startswith("index-") else f"js_chunk_{f.stem}"
            assets[role] = f
        elif f.suffix == ".css":
            role = "css_main" if f.name.startswith("index-") else f"css_extra_{f.stem}"
            assets[role] = f
        # .map files can be ignored (not served in prod)

    print("── New assets discovered ────────────────────────────")
    for role, path in sorted(assets.items()):
        print(f"  {role:20s}  {path.name}")
    print()
    return assets


def sync_assets(assets: dict[str, Path], dry_run: bool) -> None:
    """Clear old hashed files and copy new ones to StormDPS."""
    print("── Syncing assets to StormDPS ───────────────────────")

    if not dry_run:
        DEST_ASSETS.mkdir(parents=True, exist_ok=True)

        # Remove old hashed JS/CSS (keep any favicon or static assets)
        for old in DEST_ASSETS.iterdir():
            if old.suffix in (".js", ".css") and re.search(r"-[A-Za-z0-9_]{8,}\.(js|css)$", old.name):
                print(f"  removing  {old.name}")
                old.unlink()

        for role, src in assets.items():
            dest = DEST_ASSETS / src.name
            shutil.copy2(src, dest)
            print(f"  copied    {src.name}")
    else:
        print("[dry-run] Would clear old hashed .js/.css and copy new assets")
    print()


def update_index_html(assets: dict[str, Path], dry_run: bool) -> None:
    """Rewrite index.html with the new hashed filenames."""
    print("── Updating index.html ──────────────────────────────")

    if not INDEX_HTML.exists():
        print(f"ERROR: index.html not found at {INDEX_HTML}", file=sys.stderr)
        sys.exit(1)

    html = INDEX_HTML.read_text(encoding="utf-8")
    original = html

    js_main  = assets.get("js_main")
    css_main = assets.get("css_main")

    if js_main:
        # Replace any existing index-HASH.js references (preload + script src)
        html = re.sub(
            r'index-[A-Za-z0-9_]+\.js',
            js_main.name,
            html,
        )

    if css_main:
        # Replace any existing index-HASH.css references (preload + link href)
        html = re.sub(
            r'index-[A-Za-z0-9_]+\.css',
            css_main.name,
            html,
        )

    # Handle any extra JS chunks (e.g. maplibre split chunk)
    for role, path in assets.items():
        if role.startswith("js_chunk_"):
            old_stem = re.escape(path.stem.rsplit("-", 1)[0])  # strip hash
            html = re.sub(
                rf'{old_stem}-[A-Za-z0-9_]+\.js',
                path.name,
                html,
            )

    if html == original:
        print("  index.html — no changes needed (hashes already match)")
    elif dry_run:
        print("[dry-run] Would update index.html with new hashes")
        # Show a diff-style preview
        for old_line, new_line in zip(original.splitlines(), html.splitlines()):
            if old_line != new_line:
                print(f"  - {old_line.strip()}")
                print(f"  + {new_line.strip()}")
    else:
        INDEX_HTML.write_text(html, encoding="utf-8")
        print(f"  index.html updated at {INDEX_HTML}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and sync SurgeDPS frontend to StormDPS")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing anything")
    parser.add_argument("--skip-build", action="store_true", help="Skip npm build (use existing dist/)")
    args = parser.parse_args()

    print(f"\nSurgeDPS frontend sync {'[DRY RUN] ' if args.dry_run else ''}".upper())
    print(f"  Source:  {UI_DIR}")
    print(f"  Dest:    {DEST_ASSETS}")
    print(f"  HTML:    {INDEX_HTML}\n")

    if not args.skip_build:
        run_build(args.dry_run)

    assets = discover_assets(args.dry_run)

    if assets or args.dry_run:
        sync_assets(assets, args.dry_run)
        update_index_html(assets, args.dry_run)

    if not args.dry_run:
        print("✓ Done. Commit and push StormDPS to deploy.\n")
    else:
        print("✓ Dry run complete. Run without --dry-run to apply.\n")


if __name__ == "__main__":
    main()
