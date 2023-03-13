import botocore.exceptions
import requests
import zipfile
import boto3
import json
import io

# Downloading zip content
url = "http://localhost:8080/etl/v1/data_download?method=download"
response_data = requests.get(url)

# url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
# headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.63'}
# response_data = requests.get(url, headers=headers, stream=True)
# print(response_data)

response_content = response_data.content

# Retrieving the list of existing buckets
s3 = boto3.client('s3')
response = s3.list_buckets()

print('Existing buckets:')
for bucket in response['Buckets']:
    bucket_name = bucket["Name"]
    print(f'  {bucket_name}')
print(f'printing bucket name: {bucket_name}')

# Uploading the zip file contents from Web url
s3.upload_fileobj(io.BytesIO(response_content), bucket_name, "test.zip")

# To get the zip file from s3 and extract
# Getting the zip file object from s3.
z = s3.get_object(Bucket=bucket_name, Key="test.zip")

# Reading the zip file object from s3
zi = io.BytesIO(z['Body'].read())

# Uploading the file object to s3 and reading it in s3.
list_files = []
folder_name = 'unzip'
with zipfile.ZipFile(zi, mode='r') as zipf:
    for file in zipf.infolist():
        file_name = file.filename
        putFile = s3.put_object(Body=zipf.read(file), Bucket=bucket_name, Key=f"{folder_name}/{file_name}")
        list_files.append(putFile)
        print(f'In progress - {file_name}')
print("Successfully Unzipped in s3")

s3_object = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']

list_filename = []
for filename in s3_object:
    list_filename.append(filename.get('Key').split('/')[-1])


s3 = boto3.resource('s3')
bucket_name = 'sel-pro-data'
bucket = s3.Bucket(bucket_name)

file_names = []
for obj in bucket.objects.all():
    file_names.append(obj.key)
print(file_names)


def convert_to_string(data):
    if isinstance(data, float):
        return str(data)
    elif isinstance(data, dict):
        return {k: convert_to_string(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_to_string(elem) for elem in data]
    else:
        return data


# Create a boto3 client to access the DynamoDB service
dynamodb = boto3.resource('dynamodb')

# TODO: Need to create the table here itself
table = dynamodb.Table('jsonData')
print("Table object: {}".format(table))

prefix = f'{folder_name}/'

try:
    for file in list_filename:
        s3_object = s3.Object(bucket_name, prefix + file)
        s3_data = s3_object.get()['Body'].read().decode('utf-8')
        json_data = json.loads(s3_data)

        data = json.dumps(json_data, separators=(',', ':'), default=str)
        json_dict = json.loads(data)
        string_data = convert_to_string(json_dict)
        if string_data.get("cik"):
            table.put_item(
                Item=string_data
            )
            print(f'success - {file}')
        else:
            pass
except botocore.exceptions.ClientError as e:
    print(f"printing exceptions: {e}")

print("Data moved into DynamoDB successfully")
