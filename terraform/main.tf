terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Change from "local" to "gcs" (for Google Cloud Storage) or "s3" (for AWS S3) if you want to preserve your tfstate online
  required_providers {
    aws = {  # Change the required provider to AWS
      source  = "hashicorp/aws"
    }
  }
}

provider "aws" {
  region      = var.AWS_REGION  # Set AWS region using variable
  access_key  = var.AWS_ACCESS_KEY  # Set AWS access key using variable
  secret_key  = var.AWS_SECRET_KEY  # Set AWS secret key using variable
}
