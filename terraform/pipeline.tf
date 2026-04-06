# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Flood Modeling Pipeline — Step Functions + Lambda + Batch
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Pipeline Lambda Functions ──────────────────────────────────────

# Shared IAM role for pipeline Lambdas
resource "aws_iam_role" "pipeline_lambda" {
  name = "${local.prefix}-pipeline-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "pipeline_lambda" {
  name = "${local.prefix}-pipeline-lambda-policy"
  role = aws_iam_role.pipeline_lambda.id

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
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::noaa-nwm-pds/*", "arn:aws:s3:::usgs-lidar-public/*"]
      },
      # CloudFront invalidation (publish stage)
      {
        Effect   = "Allow"
        Action   = ["cloudfront:CreateInvalidation"]
        Resource = "arn:aws:cloudfront::*:distribution/${aws_cloudfront_distribution.main.id}"
      },
    ]
  })
}

# Placeholder for the container-based Lambda (geospatial deps)
data "archive_file" "pipeline_placeholder" {
  type        = "zip"
  output_path = "${path.module}/.build/pipeline_placeholder.zip"
  source {
    content  = "# Placeholder — deploy via container image"
    filename = "handler.py"
  }
}

# Ingest Lambda
resource "aws_lambda_function" "ingest" {
  function_name = "${local.prefix}-pipeline-ingest"
  role          = aws_iam_role.pipeline_lambda.arn
  filename      = data.archive_file.pipeline_placeholder.output_path
  source_code_hash = data.archive_file.pipeline_placeholder.output_base64sha256
  handler       = "pipeline.orchestrator.lambda_ingest"
  runtime       = "python3.12"
  memory_size   = 4096
  timeout       = 600
  ephemeral_storage { size = 10240 }
  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.data.bucket
      DRY_RUN     = "false"
    }
  }
}

# Model Lambda
resource "aws_lambda_function" "model" {
  function_name = "${local.prefix}-pipeline-model"
  role          = aws_iam_role.pipeline_lambda.arn
  filename      = data.archive_file.pipeline_placeholder.output_path
  source_code_hash = data.archive_file.pipeline_placeholder.output_base64sha256
  handler       = "pipeline.orchestrator.lambda_model"
  runtime       = "python3.12"
  memory_size   = 8192
  timeout       = 900
  ephemeral_storage { size = 10240 }
  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.data.bucket
      DRY_RUN     = "false"
    }
  }
}

# TileGen Lambda
resource "aws_lambda_function" "tilegen" {
  function_name = "${local.prefix}-pipeline-tilegen"
  role          = aws_iam_role.pipeline_lambda.arn
  filename      = data.archive_file.pipeline_placeholder.output_path
  source_code_hash = data.archive_file.pipeline_placeholder.output_base64sha256
  handler       = "pipeline.orchestrator.lambda_tilegen"
  runtime       = "python3.12"
  memory_size   = 8192
  timeout       = 900
  ephemeral_storage { size = 10240 }
  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.data.bucket
      DRY_RUN     = "false"
    }
  }
}

# Publish Lambda
resource "aws_lambda_function" "publish" {
  function_name = "${local.prefix}-pipeline-publish"
  role          = aws_iam_role.pipeline_lambda.arn
  filename      = data.archive_file.pipeline_placeholder.output_path
  source_code_hash = data.archive_file.pipeline_placeholder.output_base64sha256
  handler       = "pipeline.orchestrator.lambda_publish"
  runtime       = "python3.12"
  memory_size   = 2048
  timeout       = 300
  ephemeral_storage { size = 2048 }
  environment {
    variables = {
      DATA_BUCKET                = aws_s3_bucket.data.bucket
      CLOUDFRONT_DISTRIBUTION_ID = aws_cloudfront_distribution.main.id
      TILE_BASE_URL              = "https://${var.domain_name != "" ? var.domain_name : aws_cloudfront_distribution.main.domain_name}"
      DRY_RUN                    = "false"
    }
  }
}

# ── Step Functions State Machine ───────────────────────────────────

resource "aws_iam_role" "step_functions" {
  name = "${local.prefix}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.prefix}-sfn-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.ingest.arn,
        aws_lambda_function.model.arn,
        aws_lambda_function.tilegen.arn,
        aws_lambda_function.publish.arn,
      ]
    }]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.prefix}-flood-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/../src/pipeline/state_machine.json", {
    IngestLambdaArn  = aws_lambda_function.ingest.arn
    ModelLambdaArn   = aws_lambda_function.model.arn
    TileGenLambdaArn = aws_lambda_function.tilegen.arn
    PublishLambdaArn = aws_lambda_function.publish.arn
  })
}

# ── Outputs ────────────────────────────────────────────────────────

output "pipeline_state_machine_arn" {
  value       = aws_sfn_state_machine.pipeline.arn
  description = "ARN of the Step Functions pipeline (pass to storm detector)"
}

output "ingest_lambda_arn" {
  value = aws_lambda_function.ingest.arn
}

output "model_lambda_arn" {
  value = aws_lambda_function.model.arn
}

output "tilegen_lambda_arn" {
  value = aws_lambda_function.tilegen.arn
}

output "publish_lambda_arn" {
  value = aws_lambda_function.publish.arn
}
