import csv
import json
import requests
from datetime import datetime
import sys
import os
import glob
import boto3
import shutil

class TestObject:

    def __init__(self, order, category, method):
        self.order = order
        self.category = category
        self.method = method

def download_from_s3_directory(bucket, directory):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket)
    for object in bucket.objects.filter(Prefix = directory):
        if not os.path.exists(os.path.dirname(object.key)):
            os.makedirs(os.path.dirname(object.key))
        bucket.download_file(object.key, object.key)


def upload_to_aws_s3(local_file, bucket, s3_file):
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

def delete_from_s3(bucket, s3_file_name):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, s3_file_name)
    obj.delete()


def data_upload(input_file_path, output_file, error_file):
    with open(input_file_path, 'r') as in_csv, open(output_file, 'wb') as out_csv, open(error_file, 'wb') as err_csv:
        reader = csv.reader(in_csv)
        header = reader.next()
        header.append('remark')
        out_writer = csv.writer(out_csv)
        out_writer.writerow(header)
        err_writer = csv.writer(err_csv)
        err_writer.writerow(header)

        for order, category, sampling_pct, failure_threshold_pct, method in reader:
            empty = ['', '', '']
            non_empty = [int(order), category, method]
            payload = TestObject(int(order), category, method)
            response = requests.post(url, data=json.dumps(payload.__dict__), headers=headers)
            if response.status_code == 200:
                non_empty.append(response.json()['display_message'])
                out_writer.writerow(non_empty)
                err_writer.writerow(empty)
            else:
                non_empty.append(response.json()['display_message'])
                out_writer.writerow(empty)
                err_writer.writerow(non_empty)


headers = {'Content-type': 'application/json'}
path = 'test/entities/testobject/input/'
out_dir = path.replace('input', 'output')
err_dir = path.replace('input', 'error')
os.makedirs(out_dir)
os.makedirs(err_dir)
url = sys.argv[1]
bucket = 'bucket-name'

files = 'test/entities/testobject/input/*.csv'
download_from_s3_directory(bucket, path)
for input_file_path in glob.glob(files):
    file_name = input_file_path.split('/')[-1]
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    output_file = out_dir + file_name.split('.')[0] + '-' + now + '.csv'
    error_file = err_dir + file_name.split('.')[0] + '-' + now + '.csv'
    data_upload(input_file_path, output_file, error_file)
    upload_to_aws_s3(output_file, bucket, output_file)
    upload_to_aws_s3(error_file, bucket, error_file)
    delete_from_s3(bucket, input_file_path)


shutil.rmtree('grn')
