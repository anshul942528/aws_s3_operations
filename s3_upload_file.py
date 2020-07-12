import boto3
from botocore.exceptions import NoCredentialsError
import sys
import errno

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except EnvironmentError as err:
        if err.errno == errno.ENOENT: # ENOENT -> "no entity" -> "file not found"
            print("Caught 'file not found' exception")
        else:
            raise
    except NoCredentialsError:
        print("Credentials not available")
        return False

input_file_path = sys.argv[1]
file_name = input_file_path.split('/')[-1]
bucket='bucket-name'
s3_file_name='test/entities/testobject/input/' + file_name
uploaded = upload_to_aws(input_file_path, bucket, s3_file_name)
