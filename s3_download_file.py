import boto3
import botocore
import os
import glob

def downloadDirectoryFroms3(bucket, directory):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    for object in bucket.objects.filter(Prefix = directory):
        if not os.path.exists(os.path.dirname(object.key)):
            os.makedirs(os.path.dirname(object.key))
        bucket.download_file(object.key, object.key)

bucket = 'bucket-name'
directory = 'vim/response/'
downloadDirectoryFroms3(bucket, directory)
path = 'vim/response/*.csv'
for fname in glob.glob(path):
    print(fname)

#print(directory.replace('response', 'request'))
#print(directory.replace('response', 'input'))
