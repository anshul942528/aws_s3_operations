import boto3
import sys

s3 = boto3.resource('s3')
bucket='bucket-name'
my_bucket = s3.Bucket(bucket)

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object)
