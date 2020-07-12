import boto3

s3 = boto3.resource('s3')
# First parameter bucket name 
# Second is file name with directory
obj = s3.Object('bucket-name', 'test/entities/input/input-2020-06-29T17:41:40.csv')
obj.delete()
