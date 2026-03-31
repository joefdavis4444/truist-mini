# ── Athena workgroup ─────────────────────────────────────
resource "aws_athena_workgroup" "primary" {
  name = "primary"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
    }
  }

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}
