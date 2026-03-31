# ── Bronze bucket — raw landing zone ────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project}-bronze-${var.initials}"

  tags = {
    Project     = var.project
    Layer       = "bronze"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-after-24-months"
    status = "Enabled"

    transition {
      days          = 730
      storage_class = "GLACIER"
    }
  }
}

# ── Silver bucket — transformed data ────────────────────
resource "aws_s3_bucket" "silver" {
  bucket = "${var.project}-silver-${var.initials}"

  tags = {
    Project     = var.project
    Layer       = "silver"
    Environment = "dev"
  }
}

# ── Gold bucket — dimensional model ─────────────────────
resource "aws_s3_bucket" "gold" {
  bucket = "${var.project}-gold-${var.initials}"

  tags = {
    Project     = var.project
    Layer       = "gold"
    Environment = "dev"
  }
}

# ── Athena results bucket ────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project}-athena-results-${var.initials}"

  tags = {
    Project     = var.project
    Layer       = "athena"
    Environment = "dev"
  }
}

# ── Glue scripts prefix inside bronze ───────────────────
# Note: S3 "folders" are just key prefixes — no resource needed.
# Scripts live at s3://truist-mini-bronze-jfd/glue-scripts/
# Source DB lives at s3://truist-mini-bronze-jfd/source/
