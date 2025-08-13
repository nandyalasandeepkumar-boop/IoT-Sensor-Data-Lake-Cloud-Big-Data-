output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "ingest_url" {
  value = aws_lambda_function_url.ingest_url.function_url
}
