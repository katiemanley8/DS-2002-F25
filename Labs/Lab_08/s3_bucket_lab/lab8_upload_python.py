import boto3
import requests
import sys

# Check command-line arguments
if len(sys.argv) != 4:
    print("Usage: python lab8_upload_python.py <file_url> <bucket_name> <expires_in_seconds>")
    sys.exit(1)

file_url = sys.argv[1]
bucket_name = sys.argv[2]
expires_in = int(sys.argv[3])

# Extract filename from URL
filename = file_url.split("/")[-1]

# Step 1: Download the file from the internet
print(f"Downloading {filename}...")
response = requests.get(file_url)
with open(filename, "wb") as f:
    f.write(response.content)

# Step 2: Create an S3 client
s3 = boto3.client("s3", region_name="us-east-1")

# Step 3: Upload file to S3
print("Uploading file to S3...")
s3.upload_file(Filename=filename, Bucket=bucket_name, Key=filename)

# Step 4: Generate a presigned URL
print("Generating presigned URL...")
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket_name, "Key": filename},
    ExpiresIn=expires_in
)

print(f"Presigned URL (expires in {expires_in} seconds):\n{url}")

