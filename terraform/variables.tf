variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "truist-mini"
}

variable "initials" {
  description = "Your initials used for globally unique bucket names"
  type        = string
  default     = "jfd"
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
  default     = "164047822679"
}
