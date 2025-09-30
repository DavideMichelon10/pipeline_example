terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.5.0"
}

provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "my_bucket" {
  bucket = "openmeteo3423534654"
  force_destroy = true #cancella tutto quando fai terraform destroy
}

resource "aws_iam_user" "s3_user" {
  name = "s3-uploader"
}

resource "aws_iam_access_key" "s3_user_key" {
  user = aws_iam_user.s3_user.name
}

data "aws_iam_policy_document" "s3_policy" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.my_bucket.arn,
      "${aws_s3_bucket.my_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name   = "s3-uploader-policy"
  policy = data.aws_iam_policy_document.s3_policy.json
}

resource "aws_iam_user_policy_attachment" "s3_attach" {
  user       = aws_iam_user.s3_user.name
  policy_arn = aws_iam_policy.s3_policy.arn
}
