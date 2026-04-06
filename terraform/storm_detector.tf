# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Storm Detector — Lambda + EventBridge + DynamoDB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# This deploys:
#   - Lambda function for the storm detector
#   - EventBridge rule to invoke it every 15 minutes
#   - DynamoDB table for advisory state tracking
#   - S3 bucket for storm data (shared with downstream pipeline)
#   - IAM roles and policies
#
# Usage:
#   cd terraform/
#   terraform init
#   terraform plan -var="environment=dev"
#   terraform apply -var="environment=dev"
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "SurgeDPS"
      Component   = "StormDetector"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ── Variables ──────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "surgedps"
}

variable "active_basins" {
  description = "Comma-separated basin codes to monitor"
  type        = string
  default     = "at"
}

variable "poll_interval_minutes" {
  description = "How often to check NHC feeds (minutes)"
  type        = number
  default     = 15
}

variable "pipeline_state_machine_arn" {
  description = "ARN of the flood modeling Step Functions state machine"
  type        = string
  default     = ""
}

locals {
  prefix     = "${var.project_name}-${var.environment}"
  lambda_name = "${local.prefix}-storm-detector"
}

# ── DynamoDB Table ─────────────────────────────────────────────────

resource "aws_dynamodb_table" "advisory_state" {
  name         = "${local.prefix}-advisory-state"
  billing_mode = "PAY_PER_REQUEST" # On-demand: $0 when idle

  hash_key  = "storm_id"
  range_key = "advisory_guid"

  attribute {
    name = "storm_id"
    type = "S"
  }

  attribute {
    name = "advisory_guid"
    type = "S"
  }

  # Auto-expire old items after 90 days
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# ── S3 Bucket ──────────────────────────────────────────────────────

resource "aws_s3_bucket" "data" {
  bucket = "${local.prefix}-data"
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # Move raw storm data to Glacier after 90 days
  rule {
    id     = "archive-raw-data"
    status = "Enabled"

    filter {
      prefix = "storms/"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM Role for Lambda ───────────────────────────────────────────

resource "aws_iam_role" "storm_detector" {
  name = "${local.lambda_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "storm_detector" {
  name = "${local.lambda_name}-policy"
  role = aws_iam_role.storm_detector.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # CloudWatch Logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
      # DynamoDB (read/write advisory state)
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:Scan"
        ]
        Resource = aws_dynamodb_table.advisory_state.arn
      },
      # S3 (write storm GIS data)
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${aws_s3_bucket.data.arn}/storms/*"
      },
      # Step Functions (trigger pipeline)
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = var.pipeline_state_machine_arn != "" ? var.pipeline_state_machine_arn : "*"
      }
    ]
  })
}

# ── Lambda Function ────────────────────────────────────────────────

# NOTE: In production, you would build and upload a deployment package.
# This placeholder uses a dummy zip; replace with your CI/CD build.

data "archive_file" "storm_detector_placeholder" {
  type        = "zip"
  output_path = "${path.module}/.build/storm_detector_placeholder.zip"

  source {
    content  = "# Placeholder — replace with real deployment package"
    filename = "handler.py"
  }
}

resource "aws_lambda_function" "storm_detector" {
  function_name = local.lambda_name
  role          = aws_iam_role.storm_detector.arn

  # Replace this with your actual deployment package
  filename         = data.archive_file.storm_detector_placeholder.output_path
  source_code_hash = data.archive_file.storm_detector_placeholder.output_base64sha256

  handler = "handler.lambda_handler"
  runtime = "python3.12"

  memory_size = 512  # MB — sufficient for RSS parsing + small downloads
  timeout     = 120  # seconds — 2 min for feed fetch + GIS downloads
  ephemeral_storage {
    size = 1024 # MB — for extracting shapefiles
  }

  environment {
    variables = {
      STATE_TABLE_NAME          = aws_dynamodb_table.advisory_state.name
      DATA_BUCKET               = aws_s3_bucket.data.bucket
      PIPELINE_STATE_MACHINE_ARN = var.pipeline_state_machine_arn
      ACTIVE_BASINS             = var.active_basins
      LOG_LEVEL                 = "INFO"
    }
  }
}

# ── EventBridge Schedule ───────────────────────────────────────────

resource "aws_scheduler_schedule" "storm_detector" {
  name       = "${local.lambda_name}-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(${var.poll_interval_minutes} minutes)"

  target {
    arn      = aws_lambda_function.storm_detector.arn
    role_arn = aws_iam_role.eventbridge_invoke.arn

    input = jsonencode({
      source = "scheduled"
    })
  }
}

resource "aws_iam_role" "eventbridge_invoke" {
  name = "${local.lambda_name}-eb-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_invoke" {
  name = "${local.lambda_name}-eb-policy"
  role = aws_iam_role.eventbridge_invoke.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.storm_detector.arn
    }]
  })
}

# ── CloudWatch Alarm ───────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "detector_errors" {
  alarm_name          = "${local.lambda_name}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 900 # 15 min
  statistic           = "Sum"
  threshold           = 2
  alarm_description   = "Storm detector Lambda is failing"

  dimensions = {
    FunctionName = aws_lambda_function.storm_detector.function_name
  }
}

# ── Budget Alert ───────────────────────────────────────────────────

resource "aws_budgets_budget" "monthly" {
  name         = "${local.prefix}-monthly-budget"
  budget_type  = "COST"
  limit_amount = "30"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [] # Add your email here
  }
}

# ── Outputs ────────────────────────────────────────────────────────

output "lambda_function_name" {
  value = aws_lambda_function.storm_detector.function_name
}

output "lambda_function_arn" {
  value = aws_lambda_function.storm_detector.arn
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.advisory_state.name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data.bucket
}
