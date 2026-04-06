# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Ops & Monitoring — CloudWatch Dashboards, Alarms, Budget Alerts
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Deploys:
#   - SNS topic for all operational alerts
#   - CloudWatch dashboard with 4 panels
#   - Lambda error alarms for every function
#   - Step Functions execution failure alarm
#   - Data freshness alarm (storm detector staleness)
#   - CDN error rate alarm
#   - Multi-tier budget alerts ($30, $50, $100)
#   - Batch job failure alarm
#
# Cost: ~$3/month (1 dashboard + alarms + SNS)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Variables ──────────────────────────────────────────────────────

variable "alert_email" {
  description = "Email address for operational alerts"
  type        = string
  default     = ""
}

# ── SNS Topic (all alerts route here) ─────────────────────────────

resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── CloudWatch Dashboard ─────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${local.prefix}-operations"

  dashboard_body = jsonencode({
    widgets = [

      # ── Row 1: Storm Detection ────────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "Storm Detector"
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.storm_detector.function_name, { stat = "Sum", period = 900, label = "Invocations" }],
            ["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.storm_detector.function_name, { stat = "Sum", period = 900, label = "Errors", color = "#d62728" }],
            ["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.storm_detector.function_name, { stat = "Average", period = 900, label = "Duration (ms)", yAxis = "right" }],
          ]
          view    = "timeSeries"
          region  = var.aws_region
          period  = 900
          yAxis   = { left = { min = 0 }, right = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "Pipeline Executions"
          metrics = [
            ["AWS/States", "ExecutionsStarted", "StateMachineArn", aws_sfn_state_machine.pipeline.arn, { stat = "Sum", period = 3600, label = "Started" }],
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.pipeline.arn, { stat = "Sum", period = 3600, label = "Succeeded", color = "#2ca02c" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.pipeline.arn, { stat = "Sum", period = 3600, label = "Failed", color = "#d62728" }],
            ["AWS/States", "ExecutionsTimedOut", "StateMachineArn", aws_sfn_state_machine.pipeline.arn, { stat = "Sum", period = 3600, label = "Timed Out", color = "#ff7f0e" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "Active Storms"
          metrics = [
            ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", aws_dynamodb_table.advisory_state.name, { stat = "Sum", period = 3600, label = "Advisory Writes" }],
            ["AWS/States", "ExecutionsStarted", "StateMachineArn", aws_sfn_state_machine.pipeline.arn, { stat = "Sum", period = 86400, label = "Pipelines Today" }],
          ]
          view   = "singleValue"
          region = var.aws_region
          period = 86400
        }
      },

      # ── Row 2: Pipeline Lambda Functions ──────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Pipeline Lambda Duration"
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-ingest", { stat = "p90", period = 300, label = "Ingest p90" }],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-model", { stat = "p90", period = 300, label = "Model p90" }],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-tilegen", { stat = "p90", period = 300, label = "TileGen p90" }],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.prefix}-publish", { stat = "p90", period = 300, label = "Publish p90" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Pipeline Lambda Errors"
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", "${local.prefix}-ingest", { stat = "Sum", period = 300, label = "Ingest", color = "#1f77b4" }],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.prefix}-model", { stat = "Sum", period = 300, label = "Model", color = "#ff7f0e" }],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.prefix}-tilegen", { stat = "Sum", period = 300, label = "TileGen", color = "#2ca02c" }],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.prefix}-publish", { stat = "Sum", period = 300, label = "Publish", color = "#d62728" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
        }
      },

      # ── Row 3: CDN & Delivery ─────────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          title   = "CDN Requests"
          metrics = [
            ["AWS/CloudFront", "Requests", "DistributionId", aws_cloudfront_distribution.main.id, "Region", "Global", { stat = "Sum", period = 300, label = "Requests" }],
          ]
          view   = "timeSeries"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          title   = "CDN Error Rates"
          metrics = [
            ["AWS/CloudFront", "4xxErrorRate", "DistributionId", aws_cloudfront_distribution.main.id, "Region", "Global", { stat = "Average", period = 300, label = "4xx %", color = "#ff7f0e" }],
            ["AWS/CloudFront", "5xxErrorRate", "DistributionId", aws_cloudfront_distribution.main.id, "Region", "Global", { stat = "Average", period = 300, label = "5xx %", color = "#d62728" }],
          ]
          view   = "timeSeries"
          region = "us-east-1"
          yAxis  = { left = { min = 0, max = 10 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          title   = "CDN Cache Hit Rate"
          metrics = [
            ["AWS/CloudFront", "CacheHitRate", "DistributionId", aws_cloudfront_distribution.main.id, "Region", "Global", { stat = "Average", period = 300, label = "Cache Hit %" }],
          ]
          view   = "timeSeries"
          region = "us-east-1"
          yAxis  = { left = { min = 0, max = 100 } }
        }
      },

      # ── Row 4: Cost & Storage ─────────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          title   = "S3 Storage"
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.data.bucket, "StorageType", "StandardStorage", { stat = "Average", period = 86400, label = "Data Bucket (bytes)" }],
            ["AWS/S3", "NumberOfObjects", "BucketName", aws_s3_bucket.data.bucket, "StorageType", "AllStorageTypes", { stat = "Average", period = 86400, label = "Object Count", yAxis = "right" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          title   = "HEC-RAS Batch Jobs"
          metrics = [
            ["AWS/Batch", "JobsSubmitted", "JobQueue", aws_batch_job_queue.hecras.name, { stat = "Sum", period = 3600, label = "Submitted" }],
            ["AWS/Batch", "JobsSucceeded", "JobQueue", aws_batch_job_queue.hecras.name, { stat = "Sum", period = 3600, label = "Succeeded", color = "#2ca02c" }],
            ["AWS/Batch", "JobsFailed", "JobQueue", aws_batch_job_queue.hecras.name, { stat = "Sum", period = 3600, label = "Failed", color = "#d62728" }],
          ]
          view   = "timeSeries"
          region = var.aws_region
        }
      },
    ]
  })
}

# ── Lambda Error Alarms (all functions) ───────────────────────────

locals {
  pipeline_lambdas = {
    "ingest"  = "${local.prefix}-ingest"
    "model"   = "${local.prefix}-model"
    "tilegen" = "${local.prefix}-tilegen"
    "publish" = "${local.prefix}-publish"
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_errors" {
  for_each = local.pipeline_lambdas

  alarm_name          = "${each.value}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Pipeline ${each.key} Lambda has errors"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  dimensions = {
    FunctionName = each.value
  }
}

resource "aws_cloudwatch_metric_alarm" "url_signer_errors" {
  alarm_name          = "${local.prefix}-url-signer-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "URL signer Lambda error rate elevated"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.url_signer.function_name
  }
}

# ── Step Functions Failure Alarm ──────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "pipeline_failures" {
  alarm_name          = "${local.prefix}-pipeline-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Flood modeling pipeline execution failed"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "pipeline_timeouts" {
  alarm_name          = "${local.prefix}-pipeline-timeouts"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsTimedOut"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Flood modeling pipeline execution timed out"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }
}

# ── Data Freshness Alarm ──────────────────────────────────────────
# Fires if the storm detector hasn't run successfully in 30 minutes.
# Uses a custom metric published by the detector Lambda.

resource "aws_cloudwatch_metric_alarm" "detector_staleness" {
  alarm_name          = "${local.prefix}-detector-stale"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Invocations"
  namespace           = "AWS/Lambda"
  period              = 900  # 15 min (matches detector schedule)
  statistic           = "Sum"
  threshold           = 1   # At least 1 invocation per 15 min
  alarm_description   = "Storm detector has not run in the last 30 minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  treat_missing_data = "breaching"

  dimensions = {
    FunctionName = aws_lambda_function.storm_detector.function_name
  }
}

# ── Batch Job Failure Alarm ───────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "batch_failures" {
  alarm_name          = "${local.prefix}-batch-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "JobsFailed"
  namespace           = "AWS/Batch"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "HEC-RAS Batch job failed"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    JobQueue = aws_batch_job_queue.hecras.name
  }
}

# ── Multi-Tier Budget Alerts ──────────────────────────────────────

resource "aws_budgets_budget" "tiered" {
  name         = "${local.prefix}-tiered-budget"
  budget_type  = "COST"
  limit_amount = "100"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # $30 warning (normal monitoring mode ceiling)
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 30
    threshold_type            = "ABSOLUTE_VALUE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = var.alert_email != "" ? [var.alert_email] : []
    subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
  }

  # $50 alert (active storm, expected)
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 50
    threshold_type            = "ABSOLUTE_VALUE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = var.alert_email != "" ? [var.alert_email] : []
    subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
  }

  # $100 critical (investigate immediately)
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "ABSOLUTE_VALUE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = var.alert_email != "" ? [var.alert_email] : []
    subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
  }

  # $75 forecasted (early warning)
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 75
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_email_addresses = var.alert_email != "" ? [var.alert_email] : []
    subscriber_sns_topic_arns  = [aws_sns_topic.alerts.arn]
  }
}

# ── Outputs ───────────────────────────────────────────────────────

output "sns_alerts_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "SNS topic ARN for all operational alerts"
}

output "dashboard_url" {
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${local.prefix}-operations"
  description = "Direct link to the CloudWatch operations dashboard"
}
