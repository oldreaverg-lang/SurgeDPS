#!/usr/bin/env bash
# deploy.sh — Build SurgeDPS and stage into StormDPS deploy repo.
# Handles git index corruption automatically.
#
# Usage:  bash deploy.sh [path_to_stormdps_repo]
#   Default StormDPS path: ../StormDPS

set -euo pipefail

SURGE_UI="$(cd "$(dirname "$0")/ui" && pwd)"
STORM_REPO="${1:-$(cd "$(dirname "$0")/../StormDPS" && pwd)}"
DEPLOY_DIR="$STORM_REPO/frontend/surgedps"
TITLE="SurgeDPS — Storm Surge Analysis"

echo "── Build ──────────────────────────────────────────────"
echo "Source:  $SURGE_UI/src/App.tsx"
echo "Deploy:  $DEPLOY_DIR"

# 1. Build
cd "$SURGE_UI"
npm run build 2>&1

# 2. Clean old hashed assets
echo ""
echo "── Deploy ─────────────────────────────────────────────"
rm -f "$DEPLOY_DIR/assets/"*.js "$DEPLOY_DIR/assets/"*.css
echo "Cleaned old assets."

# 3. Copy new build output
cp "$SURGE_UI/dist/assets/"* "$DEPLOY_DIR/assets/"
cp "$SURGE_UI/dist/index.html" "$DEPLOY_DIR/index.html"
echo "Copied new build output."

# 4. Fix Vite's default <title>
sed -i "s|<title>ui</title>|<title>$TITLE</title>|" "$DEPLOY_DIR/index.html"
echo "Fixed page title."

# 5. Stage in StormDPS with git-index auto-recovery
echo ""
echo "── Git stage ──────────────────────────────────────────"
cd "$STORM_REPO"

stage_files() {
  git add frontend/surgedps/assets/ frontend/surgedps/index.html
}

if ! stage_files 2>/dev/null; then
  echo "Git index corrupted — auto-recovering..."
  rm -f .git/index
  git reset HEAD -- . 2>/dev/null || true
  stage_files
  echo "Recovery complete."
else
  echo "Files staged."
fi

echo ""
echo "── Done ───────────────────────────────────────────────"
git -c color.status=always status --short frontend/surgedps/
echo ""
echo "Ready to commit. Review above, then:"
echo "  cd $STORM_REPO"
echo '  git commit -m "Deploy SurgeDPS update"'
echo '  git push'
