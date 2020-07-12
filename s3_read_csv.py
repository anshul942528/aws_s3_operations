import csv
import json
import requests
from datetime import datetime
import sys
import boto3
import smart_open

class TestObject:

    def __init__(self, category, pid):
        self.category = category
        self.product_id = pid


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

headers = {'Content-type': 'application/json'}
path = 'test/entities'

input_file_path = sys.argv[1]
url = sys.argv[2]
bucket=sys.argv[3]

file_name = input_file_path.split('/')[-1]
now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
s3_file_name = path + '/input/' + file_name.split('.')[0] + '-' + now + '.csv'
output_file = path + '/output/' + file_name.split('.')[0] + '-' + now + '.csv'
error_file = path + '/error/' + file_name.split('.')[0] + '-' + now + '.csv'

uploaded = upload_to_aws(input_file_path, bucket, s3_file_name)

with smart_open.smart_open('s3://mybucket/' + bucket + s3_file_name, 'r') as in_csv, smart_open.smart_open('s3://mybucket/' + bucket + output_file, 'wb') as out_csv, smart_open.smart_open('s3://mybucket/' + bucket + error_file, 'wb') as err_csv:
    reader = csv.reader(in_csv)
    header = reader.next()
    header.append('remark')
    out_writer = csv.writer(out_csv)
    out_writer.writerow(header)
    err_writer = csv.writer(err_csv)
    err_writer.writerow(header)

    for category, pid, vendor_code, brand, sampling_pct, max_sampling_qty, failure_threshold_pct in reader:
        empty = ['', '', '', '', '', '', '']
        non_empty = [category, pid, vendor_code, brand, int(sampling_pct), int(max_sampling_qty), int(failure_threshold_pct)]
        payload = QcMaster(category, pid, vendor_code, brand, int(sampling_pct), int(max_sampling_qty), int(failure_threshold_pct))
        response = requests.post(url, data=json.dumps(payload.__dict__), headers=headers)
        #if response.status_code == 200:
        non_empty.append(response.json()['display_message'])
        out_writer.writerow(non_empty)
        err_writer.writerow(empty)
        #else:
        #non_empty.append(response.json()['display_message'])
        #out_writer.writerow(empty)
        #err_writer.writerow(non_empty)
