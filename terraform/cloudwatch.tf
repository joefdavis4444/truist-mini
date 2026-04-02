# ── SNS topic for alerts ─────────────────────────────────
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}

# ── Glue batch job failure alarm ─────────────────────────
resource "aws_cloudwatch_metric_alarm" "glue_batch_failure" {
  alarm_name          = "${var.project}-glue-batch-failure"
  alarm_description   = "Fires when loan_master_batch Glue job fails"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    JobName = "${var.project}-loan-master-batch"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}

# ── Glue silver job failure alarm ────────────────────────
resource "aws_cloudwatch_metric_alarm" "glue_silver_failure" {
  alarm_name          = "${var.project}-glue-silver-failure"
  alarm_description   = "Fires when silver Glue job fails"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    JobName = "${var.project}-silver-loan-master"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}

# ── Consumer pod restart alarm ───────────────────────────
resource "aws_cloudwatch_metric_alarm" "consumer_pod_restarts" {
  alarm_name          = "${var.project}-consumer-pod-restarts"
  alarm_description   = "Fires when consumer pod restart count exceeds threshold"
  metric_name         = "container.restartCount"
  namespace           = "ContainerInsights"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 3
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    ClusterName = var.project
    Namespace   = "data-platform"
  }

  alarm_actions = [aws_sns_topic.alerts.arn]

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}
