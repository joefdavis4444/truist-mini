# ── Glue Catalog database ────────────────────────────────
resource "aws_glue_catalog_database" "truist_mini" {
  name        = "truist_mini"
  description = "Truist Mini Platform data catalog"
}

# ── Batch extraction job (Python Shell) ──────────────────
# Simulates Glue JDBC job extracting from core banking Oracle.
# Uses DynamoDB watermark for incremental extraction.
# Production equivalent: kafka.m5.large JDBC against Oracle over Direct Connect.
resource "aws_glue_job" "loan_master_batch" {
  name         = "${var.project}-loan-master-batch"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "3.0"
  max_capacity = 0.0625

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/glue-scripts/loan_master_batch.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.bronze.bucket}/glue-temp/"
  }

  tags = {
    Project     = var.project
    Pipeline    = "batch"
    Layer       = "bronze"
    Environment = "dev"
  }
}

# ── Silver transformation job — loan master (PySpark) ────
resource "aws_glue_job" "loan_master_silver" {
  name              = "${var.project}-silver-loan-master"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/glue-scripts/loan_master_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"          = "python"
    "--TempDir"               = "s3://${aws_s3_bucket.bronze.bucket}/glue-temp/"
    "--enable-glue-datacatalog" = "true"
  }

  tags = {
    Project     = var.project
    Pipeline    = "batch"
    Layer       = "silver"
    Environment = "dev"
  }
}

# ── Silver transformation job — loan events (PySpark) ────
resource "aws_glue_job" "loan_events_silver" {
  name              = "${var.project}-silver-loan-events"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/glue-scripts/loan_events_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.bronze.bucket}/glue-temp/"
  }

  tags = {
    Project     = var.project
    Pipeline    = "streaming"
    Layer       = "silver"
    Environment = "dev"
  }
}

# ── Gold job — loan master (PySpark) ─────────────────────
resource "aws_glue_job" "loan_master_gold" {
  name              = "${var.project}-gold-loan-master"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/glue-scripts/loan_master_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.bronze.bucket}/glue-temp/"
  }

  tags = {
    Project     = var.project
    Pipeline    = "batch"
    Layer       = "gold"
    Environment = "dev"
  }
}

# ── Gold job — loan events (PySpark) ─────────────────────
resource "aws_glue_job" "loan_events_gold" {
  name              = "${var.project}-gold-loan-events"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/glue-scripts/loan_events_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.bronze.bucket}/glue-temp/"
  }

  tags = {
    Project     = var.project
    Pipeline    = "streaming"
    Layer       = "gold"
    Environment = "dev"
  }
}
