# ── Pipeline watermarks table ────────────────────────────
# Stores last successful extraction timestamp per source table.
# Same pattern as Truist production — transparent, queryable,
# manually resettable. Superior to Glue bookmarks which are opaque.
resource "aws_dynamodb_table" "pipeline_watermarks" {
  name         = "pipeline-watermarks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "source_table"

  attribute {
    name = "source_table"
    type = "S"
  }

  tags = {
    Project     = var.project
    Environment = "dev"
  }
}
