#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <local_file> <bucket_name> <expiration_in_seconds>"
    exit 1
fi

FILE=$1
BUCKET=$2
EXPIRE=$3

echo "Listing existing S3 buckets..."
aws s3 ls

echo "Creating bucket (if it doesn't already exist)..."
aws s3 mb s3://$BUCKET 2>/dev/null

echo "Uploading file to S3..."
aws s3 cp "$FILE" s3://$BUCKET/

echo "Listing contents of your bucket..."
aws s3 ls s3://$BUCKET/

echo "Generating presigned URL (expires in $EXPIRE seconds)..."
aws s3 presign --expires-in $EXPIRE s3://$BUCKET/$FILE
