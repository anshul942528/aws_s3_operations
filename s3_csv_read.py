import boto3
from io import StringIO
import pandas as pd

bucket = "bucket-name"
file_name = "test/entities/input/input.csv"

s3 = boto3.client('s3')
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

obj = s3.get_object(Bucket= bucket, Key= file_name)
# get object and file (key) from bucket

s3_data = StringIO(obj['Body'].read().decode('utf-8'))
data = pd.read_csv(s3_data)
print(data.head())
for r in data.head():
    print(r)
