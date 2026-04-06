# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CDN & Delivery Infrastructure — CloudFront + S3 Static Site
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# This deploys:
#   - S3 bucket for the frontend static site
#   - CloudFront distribution with three origins:
#       1. Static site (frontend HTML/JS/CSS)
#       2. Tile data (free + premium flood map tiles)
#       3. Manifest API (storm manifests)
#   - Origin Access Control (OAC) for secure S3 access
#   - CloudFront signed URL key pair for premium tiles
#   - Lambda@Edge for signed URL validation
#   - WAF v2 with rate limiting
#   - Optional Route53 + ACM for custom domain
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Variables ──────────────────────────────────────────────────────

variable "domain_name" {
  description = "Custom domain name (e.g., surgedps.com). Leave empty for CloudFront default domain."
  type        = string
  default     = ""
}

variable "enable_waf" {
  description = "Enable WAF rate limiting on the CDN"
  type        = bool
  default     = true
}

variable "free_tile_max_zoom" {
  description = "Maximum zoom level for free tiles"
  type        = number
  default     = 12
}

variable "premium_tile_max_zoom" {
  description = "Maximum zoom level for premium tiles"
  type        = number
  default     = 16
}

variable "signed_url_expiry_seconds" {
  description = "How long signed URLs for premium tiles are valid"
  type        = number
  default     = 3600
}

# ── Frontend S3 Bucket (Static Site) ──────────────────────────────

resource "aws_s3_bucket" "frontend" {
  bucket = "${local.prefix}-frontend"
}

resource "aws_s3_bucket_public_access_block" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_cors_configuration" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    max_age_seconds = 3600
  }
}

# ── Origin Access Control ─────────────────────────────────────────
# OAC replaces the legacy Origin Access Identity (OAI) for
# secure CloudFront → S3 access without making buckets public.

resource "aws_cloudfront_origin_access_control" "frontend" {
  name                              = "${local.prefix}-frontend-oac"
  description                       = "OAC for SurgeDPS frontend bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_origin_access_control" "data" {
  name                              = "${local.prefix}-data-oac"
  description                       = "OAC for SurgeDPS data bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# ── S3 Bucket Policies (allow CloudFront OAC) ────────────────────

resource "aws_s3_bucket_policy" "frontend" {
  bucket = aws_s3_bucket.frontend.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontOAC"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.frontend.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.main.arn
        }
      }
    }]
  })
}

resource "aws_s3_bucket_policy" "data_cdn" {
  bucket = aws_s3_bucket.data.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontOACRead"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.data.arn}/storms/*/tiles/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.main.arn
          }
        }
      },
      {
        Sid       = "AllowCloudFrontOACManifest"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.data.arn}/storms/*/manifest.json"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.main.arn
          }
        }
      },
    ]
  })
}

# ── CloudFront Key Group (for signed URLs) ────────────────────────
# Used to sign premium tile URLs. The public key is stored in
# CloudFront; the private key is in SSM Parameter Store for the
# signing Lambda.

resource "aws_cloudfront_public_key" "premium_tiles" {
  name        = "${local.prefix}-premium-tile-key"
  comment     = "Public key for premium tile signed URLs"
  encoded_key = var.cloudfront_public_key_pem

  lifecycle {
    # Don't destroy the key if we're just rotating — create new first
    create_before_destroy = true
  }
}

variable "cloudfront_public_key_pem" {
  description = "PEM-encoded RSA public key for CloudFront signed URLs. Generate with: openssl genrsa -out private.pem 2048 && openssl rsa -in private.pem -pubout -out public.pem"
  type        = string
  sensitive   = true
  default     = ""
}

resource "aws_cloudfront_key_group" "premium_tiles" {
  name    = "${local.prefix}-premium-tiles"
  items   = var.cloudfront_public_key_pem != "" ? [aws_cloudfront_public_key.premium_tiles.id] : []
  comment = "Key group for premium tile signed URL validation"
}

# ── Cache Policies ────────────────────────────────────────────────

resource "aws_cloudfront_cache_policy" "static_assets" {
  name        = "${local.prefix}-static-assets"
  comment     = "Cache static frontend assets aggressively"
  default_ttl = 86400   # 1 day
  max_ttl     = 2592000 # 30 days
  min_ttl     = 3600    # 1 hour

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "tile_data" {
  name        = "${local.prefix}-tile-data"
  comment     = "Cache tiles with short TTL for fresh storm data"
  default_ttl = 300     # 5 minutes — storms update every 6 hours but
  max_ttl     = 3600    # we want to serve new data quickly after publish
  min_ttl     = 60

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "whitelist"
      headers {
        items = ["Range"]  # PMTiles uses HTTP Range requests
      }
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "manifest" {
  name        = "${local.prefix}-manifest"
  comment     = "Short TTL for storm manifests (frontend polls these)"
  default_ttl = 60  # 1 minute
  max_ttl     = 300 # 5 minutes
  min_ttl     = 0

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

# ── Response Headers Policy ───────────────────────────────────────

resource "aws_cloudfront_response_headers_policy" "security" {
  name    = "${local.prefix}-security-headers"
  comment = "Security headers for SurgeDPS"

  cors_config {
    access_control_allow_credentials = false

    access_control_allow_headers {
      items = ["Range", "Accept-Encoding", "Authorization"]
    }

    access_control_allow_methods {
      items = ["GET", "HEAD", "OPTIONS"]
    }

    access_control_allow_origins {
      items = var.domain_name != "" ? ["https://${var.domain_name}"] : ["*"]
    }

    access_control_max_age_sec = 3600
    origin_override            = true
  }

  security_headers_config {
    content_type_options {
      override = true
    }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    strict_transport_security {
      access_control_max_age_sec = 31536000
      include_subdomains         = true
      override                   = true
      preload                    = true
    }
  }
}

# ── CloudFront Distribution ──────────────────────────────────────

resource "aws_cloudfront_distribution" "main" {
  enabled             = true
  is_ipv6_enabled     = true
  comment             = "SurgeDPS ${var.environment} — Flood Risk Map CDN"
  default_root_object = "index.html"
  price_class         = "PriceClass_100" # US, Canada, Europe only — cheapest
  http_version        = "http2and3"
  web_acl_id          = var.enable_waf ? aws_wafv2_web_acl.cdn[0].arn : null

  aliases = var.domain_name != "" ? [var.domain_name] : []

  viewer_certificate {
    # Use ACM cert if custom domain, otherwise CloudFront default
    acm_certificate_arn      = var.domain_name != "" ? aws_acm_certificate.cdn[0].arn : null
    cloudfront_default_certificate = var.domain_name == "" ? true : false
    minimum_protocol_version = var.domain_name != "" ? "TLSv1.2_2021" : "TLSv1"
    ssl_support_method       = var.domain_name != "" ? "sni-only" : null
  }

  # ── Origin 1: Frontend Static Site ───────────────────────────
  origin {
    domain_name              = aws_s3_bucket.frontend.bucket_regional_domain_name
    origin_id                = "frontend"
    origin_access_control_id = aws_cloudfront_origin_access_control.frontend.id
  }

  # ── Origin 2: Tile Data (S3 data bucket) ─────────────────────
  origin {
    domain_name              = aws_s3_bucket.data.bucket_regional_domain_name
    origin_id                = "tiles"
    origin_access_control_id = aws_cloudfront_origin_access_control.data.id
    origin_path              = "" # Full bucket access; path routing via behaviors
  }

  # ── Default Behavior: Frontend ───────────────────────────────
  default_cache_behavior {
    target_origin_id       = "frontend"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id            = aws_cloudfront_cache_policy.static_assets.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.security.id
  }

  # ── Behavior: Free Tiles (z8-12) ────────────────────────────
  ordered_cache_behavior {
    path_pattern           = "/storms/*/tiles/free/*"
    target_origin_id       = "tiles"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id            = aws_cloudfront_cache_policy.tile_data.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.security.id
  }

  # ── Behavior: Premium Tiles (z13-16, signed URLs) ───────────
  ordered_cache_behavior {
    path_pattern           = "/storms/*/tiles/premium/*"
    target_origin_id       = "tiles"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id            = aws_cloudfront_cache_policy.tile_data.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.security.id

    # Require signed URLs for premium tiles
    trusted_key_groups = var.cloudfront_public_key_pem != "" ? [aws_cloudfront_key_group.premium_tiles.id] : []
  }

  # ── Behavior: Storm Manifests ────────────────────────────────
  ordered_cache_behavior {
    path_pattern           = "/storms/*/manifest.json"
    target_origin_id       = "tiles"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id            = aws_cloudfront_cache_policy.manifest.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.security.id
  }

  # ── Behavior: PMTiles (Range request support) ────────────────
  ordered_cache_behavior {
    path_pattern           = "/storms/*/tiles/*.pmtiles"
    target_origin_id       = "tiles"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = false  # PMTiles are already compressed

    cache_policy_id            = aws_cloudfront_cache_policy.tile_data.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.security.id
  }

  # ── Custom Error Responses ───────────────────────────────────
  # SPA fallback: return index.html for 404s on frontend routes
  custom_error_response {
    error_code            = 403
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }

  custom_error_response {
    error_code            = 404
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 10
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  tags = {
    Name = "${local.prefix}-cdn"
  }
}

# ── ACM Certificate (optional, for custom domain) ────────────────

resource "aws_acm_certificate" "cdn" {
  count = var.domain_name != "" ? 1 : 0

  # ACM certs for CloudFront MUST be in us-east-1
  provider          = aws
  domain_name       = var.domain_name
  validation_method = "DNS"

  subject_alternative_names = [
    "*.${var.domain_name}"
  ]

  lifecycle {
    create_before_destroy = true
  }
}

# ── WAF v2 (Rate Limiting) ───────────────────────────────────────

resource "aws_wafv2_web_acl" "cdn" {
  count = var.enable_waf ? 1 : 0

  name        = "${local.prefix}-cdn-waf"
  description = "Rate limiting and basic protection for SurgeDPS CDN"
  scope       = "CLOUDFRONT"

  # WAF for CloudFront must be in us-east-1
  # (provider inherits from root — ensure var.aws_region = us-east-1)

  default_action {
    allow {}
  }

  # Rule 1: Rate limit — 2000 requests per 5 minutes per IP
  rule {
    name     = "rate-limit"
    priority = 1

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = 2000
        aggregate_key_type = "IP"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.prefix}-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  # Rule 2: Block known bad bots
  rule {
    name     = "aws-managed-bad-inputs"
    priority = 2

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesKnownBadInputsRuleGroup"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.prefix}-bad-inputs"
      sampled_requests_enabled   = true
    }
  }

  # Rule 3: Aggressive rate limit on premium tile paths
  rule {
    name     = "premium-tile-rate-limit"
    priority = 3

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = 500
        aggregate_key_type = "IP"

        scope_down_statement {
          byte_match_statement {
            search_string         = "/tiles/premium/"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "LOWERCASE"
            }
            positional_constraint = "CONTAINS"
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.prefix}-premium-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${local.prefix}-cdn-waf"
    sampled_requests_enabled   = true
  }
}

# ── Signed URL Lambda ─────────────────────────────────────────────
# This Lambda generates signed CloudFront URLs for premium tile
# access. Called by the frontend when a user unlocks premium.

resource "aws_iam_role" "url_signer" {
  name = "${local.prefix}-url-signer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "url_signer" {
  name = "${local.prefix}-url-signer-policy"
  role = aws_iam_role.url_signer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/${local.prefix}/cloudfront-private-key"
      },
    ]
  })
}

data "archive_file" "url_signer" {
  type        = "zip"
  output_path = "${path.module}/.build/url_signer.zip"
  source_dir  = "${path.module}/../src/cdn/url_signer"
}

resource "aws_lambda_function" "url_signer" {
  function_name = "${local.prefix}-url-signer"
  role          = aws_iam_role.url_signer.arn
  filename      = data.archive_file.url_signer.output_path
  source_code_hash = data.archive_file.url_signer.output_base64sha256
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  memory_size   = 256
  timeout       = 10

  environment {
    variables = {
      CLOUDFRONT_DOMAIN          = var.domain_name != "" ? var.domain_name : aws_cloudfront_distribution.main.domain_name
      CLOUDFRONT_KEY_PAIR_ID     = var.cloudfront_public_key_pem != "" ? aws_cloudfront_public_key.premium_tiles.id : ""
      PRIVATE_KEY_SSM_PARAM      = "/${local.prefix}/cloudfront-private-key"
      SIGNED_URL_EXPIRY_SECONDS  = tostring(var.signed_url_expiry_seconds)
    }
  }
}

# Lambda Function URL (no API Gateway needed — saves cost)
resource "aws_lambda_function_url" "url_signer" {
  function_name      = aws_lambda_function.url_signer.function_name
  authorization_type = "NONE"

  cors {
    allow_origins = var.domain_name != "" ? ["https://${var.domain_name}"] : ["*"]
    allow_methods = ["GET"]
    allow_headers = ["Authorization"]
    max_age       = 3600
  }
}

# ── SSM Parameter for CloudFront Private Key ──────────────────────
# The private key is stored in SSM Parameter Store (SecureString).
# Operator uploads it manually:
#   aws ssm put-parameter \
#     --name "/surgedps-dev/cloudfront-private-key" \
#     --type SecureString \
#     --value "$(cat private.pem)"

resource "aws_ssm_parameter" "cloudfront_private_key_placeholder" {
  name        = "/${local.prefix}/cloudfront-private-key"
  description = "RSA private key for CloudFront signed URLs"
  type        = "SecureString"
  value       = "PLACEHOLDER — replace with: aws ssm put-parameter --overwrite"

  lifecycle {
    ignore_changes = [value] # Don't overwrite after operator sets it
  }
}

# ── CloudWatch Alarms ─────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "cdn_5xx_errors" {
  alarm_name          = "${local.prefix}-cdn-5xx"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "5xxErrorRate"
  namespace           = "AWS/CloudFront"
  period              = 300
  statistic           = "Average"
  threshold           = 5 # 5% error rate
  alarm_description   = "CloudFront 5xx error rate is elevated"

  dimensions = {
    DistributionId = aws_cloudfront_distribution.main.id
    Region         = "Global"
  }
}

resource "aws_cloudwatch_metric_alarm" "cdn_origin_latency" {
  alarm_name          = "${local.prefix}-cdn-origin-latency"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "OriginLatency"
  namespace           = "AWS/CloudFront"
  period              = 300
  statistic           = "p90"
  threshold           = 5000 # 5 seconds p90
  alarm_description   = "CloudFront origin latency is high"

  dimensions = {
    DistributionId = aws_cloudfront_distribution.main.id
    Region         = "Global"
  }
}

# ── Outputs ────────────────────────────────────────────────────────

output "cdn_domain" {
  value       = var.domain_name != "" ? var.domain_name : aws_cloudfront_distribution.main.domain_name
  description = "Domain name to access the CDN"
}

output "cdn_distribution_id" {
  value       = aws_cloudfront_distribution.main.id
  description = "CloudFront distribution ID (for pipeline CDN invalidation)"
}

output "cdn_distribution_arn" {
  value       = aws_cloudfront_distribution.main.arn
  description = "CloudFront distribution ARN"
}

output "frontend_bucket_name" {
  value       = aws_s3_bucket.frontend.bucket
  description = "S3 bucket for frontend static site deployment"
}

output "frontend_bucket_arn" {
  value       = aws_s3_bucket.frontend.arn
  description = "Frontend bucket ARN"
}

output "url_signer_endpoint" {
  value       = aws_lambda_function_url.url_signer.function_url
  description = "URL for the signed URL generation Lambda"
}

output "tile_base_url" {
  value       = "https://${var.domain_name != "" ? var.domain_name : aws_cloudfront_distribution.main.domain_name}"
  description = "Base URL for tile access (pass to pipeline publish Lambda)"
}
