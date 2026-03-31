output "bronze_bucket" {
  description = "S3 bronze bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "S3 silver bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "S3 gold bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "glue_role_arn" {
  description = "Glue IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "watermark_table" {
  description = "DynamoDB watermark table name"
  value       = aws_dynamodb_table.pipeline_watermarks.name
}

output "glue_catalog_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.truist_mini.name
}
