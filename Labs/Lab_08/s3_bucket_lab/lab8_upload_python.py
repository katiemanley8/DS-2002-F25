import boto3
import requests

print("Downloading soccer ball image...")
image_url = "https://images.pexels.com/photos/260907/pexels-photo-260907.jpeg"
local_file_path = "crowd.jpeg"

response = requests.get(image_url)
with open(local_file_path, "wb") as f:
    f.write(response.content)
print("Image downloaded.")

s3 = boto3.client("s3", region_name="us-east-1")
bucket = "ds2002-f25-vah3xr"
s3_key = "crowd.jpeg"

print("Uploading file to S3...")
s3.upload_file(local_file_path, bucket, s3_key)
print("Upload complete.")

expires_in = 604800

print("Generating presigned URL...")
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": s3_key},
    ExpiresIn=expires_in
)

print("\nPresigned URL:")
print(url)
