# AWS
# Ref: https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket
resource "aws_s3_bucket" "bucket" {
  bucket = "magic-the-gathering-bucket"
}

resource "aws_s3_bucket_versioning" "bucket_versioning" {
  bucket = aws_s3_bucket.bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket_lifecycle_configuration" {
  bucket = aws_s3_bucket.bucket.id
  rule {
    id = "delete"
    expiration {
      days = 30  # objects will be deleted after 30 days
    }
    status = "Enabled"
  }
}
