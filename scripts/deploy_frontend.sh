#!/usr/bin/env bash
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Deploy Frontend to S3 + CloudFront
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Usage:
#   ./scripts/deploy_frontend.sh [environment]
#
# Prerequisites:
#   - AWS CLI configured
#   - Terraform outputs available
#
# This script:
#   1. Reads bucket name and distribution ID from Terraform output
#   2. Syncs frontend files to S3
#   3. Invalidates CloudFront cache
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

set -euo pipefail

ENV="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FRONTEND_DIR="$ROOT_DIR/src/frontend"
TF_DIR="$ROOT_DIR/terraform"

echo "=== SurgeDPS Frontend Deploy (${ENV}) ==="

# Get Terraform outputs
cd "$TF_DIR"
BUCKET=$(terraform output -raw frontend_bucket_name 2>/dev/null || echo "")
DIST_ID=$(terraform output -raw cdn_distribution_id 2>/dev/null || echo "")
CDN_DOMAIN=$(terraform output -raw cdn_domain 2>/dev/null || echo "")

if [ -z "$BUCKET" ]; then
    echo "ERROR: Could not read frontend_bucket_name from Terraform output."
    echo "       Run 'terraform apply' first."
    exit 1
fi

echo "  Bucket:       $BUCKET"
echo "  Distribution: $DIST_ID"
echo "  Domain:       $CDN_DOMAIN"
echo ""

# Sync frontend files to S3
echo ">>> Syncing frontend to s3://$BUCKET/ ..."
aws s3 sync "$FRONTEND_DIR" "s3://$BUCKET/" \
    --delete \
    --cache-control "public, max-age=3600" \
    --exclude "*.map" \
    --exclude ".DS_Store"

# Set correct content types for specific files
aws s3 cp "s3://$BUCKET/index.html" "s3://$BUCKET/index.html" \
    --content-type "text/html" \
    --cache-control "public, max-age=300" \
    --metadata-directive REPLACE

echo "  Uploaded $(find "$FRONTEND_DIR" -type f | wc -l | tr -d ' ') files"

# Invalidate CloudFront cache
if [ -n "$DIST_ID" ]; then
    echo ""
    echo ">>> Invalidating CloudFront cache..."
    INV_ID=$(aws cloudfront create-invalidation \
        --distribution-id "$DIST_ID" \
        --paths "/*" \
        --query 'Invalidation.Id' \
        --output text)
    echo "  Invalidation: $INV_ID"
fi

echo ""
echo "=== Deploy complete! ==="
echo "  URL: https://$CDN_DOMAIN"
