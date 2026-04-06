# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HEC-RAS Batch Compute — AWS Batch + Fargate Spot
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Deploys:
#   - ECR repository for the HEC-RAS Docker image
#   - AWS Batch compute environment (Fargate Spot for cost savings)
#   - Job queue and job definition
#   - IAM roles for Batch execution
#
# Cost model:
#   - $0/month when idle (no running containers)
#   - Fargate Spot: ~70% savings vs on-demand
#   - Typical run: 4 vCPU, 16GB RAM, 30-60 min = ~$0.15-0.30/run
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Variables ──────────────────────────────────────────────────────

variable "hecras_vcpus" {
  description = "vCPU allocation for HEC-RAS Batch jobs"
  type        = number
  default     = 4
}

variable "hecras_memory_mb" {
  description = "Memory (MB) allocation for HEC-RAS Batch jobs"
  type        = number
  default     = 16384
}

variable "hecras_timeout_seconds" {
  description = "Maximum runtime for a HEC-RAS job"
  type        = number
  default     = 3600
}

variable "hecras_ephemeral_gb" {
  description = "Ephemeral storage (GB) for HEC-RAS container"
  type        = number
  default     = 50
}

# ── ECR Repository ────────────────────────────────────────────────

resource "aws_ecr_repository" "hecras" {
  name                 = "${local.prefix}-hecras"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  # Auto-cleanup old images to save storage cost
  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_ecr_lifecycle_policy" "hecras" {
  repository = aws_ecr_repository.hecras.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep only last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# ── IAM Roles ─────────────────────────────────────────────────────

# Batch service role
resource "aws_iam_role" "batch_service" {
  name = "${local.prefix}-batch-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "batch.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

# Execution role (Fargate pulls image + writes logs)
resource "aws_iam_role" "batch_execution" {
  name = "${local.prefix}-batch-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_execution_ecr" {
  role       = aws_iam_role.batch_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Task role (what the container can do)
resource "aws_iam_role" "batch_task" {
  name = "${local.prefix}-hecras-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "batch_task" {
  name = "${local.prefix}-hecras-task-policy"
  role = aws_iam_role.batch_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # S3: read input data, write output results
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
      # CloudWatch: write container logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      },
    ]
  })
}

# ── Batch Compute Environment (Fargate Spot) ─────────────────────

resource "aws_batch_compute_environment" "hecras" {
  compute_environment_name = "${local.prefix}-hecras-compute"
  type                     = "MANAGED"
  state                    = "ENABLED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type      = "FARGATE_SPOT"
    max_vcpus = 16  # Max concurrent compute (4 simultaneous jobs)

    subnets            = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.batch.id]
  }
}

# Use default VPC for simplicity (override for production)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "batch" {
  name_prefix = "${local.prefix}-batch-"
  vpc_id      = data.aws_vpc.default.id
  description = "Security group for HEC-RAS Batch containers"

  # Outbound: allow S3 and ECR access
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS for S3 and ECR"
  }

  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP fallback"
  }
}

# ── Job Queue ─────────────────────────────────────────────────────

resource "aws_batch_job_queue" "hecras" {
  name     = "${local.prefix}-hecras-queue"
  state    = "ENABLED"
  priority = 10

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.hecras.arn
  }
}

# ── Job Definition ────────────────────────────────────────────────

resource "aws_batch_job_definition" "hecras" {
  name = "${local.prefix}-hecras-job"
  type = "container"

  platform_capabilities = ["FARGATE"]

  timeout {
    attempt_duration_seconds = var.hecras_timeout_seconds
  }

  retry_strategy {
    attempts = 2  # Retry once on Spot interruption
  }

  container_properties = jsonencode({
    image = "${aws_ecr_repository.hecras.repository_url}:6.5"

    resourceRequirements = [
      { type = "VCPU", value = tostring(var.hecras_vcpus) },
      { type = "MEMORY", value = tostring(var.hecras_memory_mb) },
    ]

    ephemeralStorage = {
      sizeInGiB = var.hecras_ephemeral_gb
    }

    executionRoleArn = aws_iam_role.batch_execution.arn
    jobRoleArn       = aws_iam_role.batch_task.arn

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/aws/batch/${local.prefix}-hecras"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "hecras"
      }
    }

    # Default environment (overridden per-job)
    environment = [
      { name = "STORM_ID", value = "UNKNOWN" },
      { name = "ADVISORY_NUM", value = "000" },
      { name = "DATA_BUCKET", value = aws_s3_bucket.data.bucket },
      { name = "TEMPLATE_NAME", value = "gulf_coast" },
    ]

    fargatePlatformConfiguration = {
      platformVersion = "LATEST"
    }
  })
}

# ── CloudWatch Log Group ──────────────────────────────────────────

resource "aws_cloudwatch_log_group" "hecras" {
  name              = "/aws/batch/${local.prefix}-hecras"
  retention_in_days = 30
}

# ── Outputs ───────────────────────────────────────────────────────

output "hecras_ecr_repository_url" {
  value       = aws_ecr_repository.hecras.repository_url
  description = "ECR URL for the HEC-RAS Docker image"
}

output "hecras_job_queue" {
  value       = aws_batch_job_queue.hecras.name
  description = "AWS Batch job queue name for HEC-RAS jobs"
}

output "hecras_job_definition" {
  value       = aws_batch_job_definition.hecras.name
  description = "AWS Batch job definition name"
}

output "hecras_job_definition_arn" {
  value       = aws_batch_job_definition.hecras.arn
  description = "AWS Batch job definition ARN"
}
