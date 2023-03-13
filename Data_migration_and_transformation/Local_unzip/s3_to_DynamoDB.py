# Wiremock - 'java -jar wiremock-jre8-standalone-2.35.0.jar'

import requests
import zipfile
import os
import json
import boto3

# Downloading zip content from Mock URL
url = "http://localhost:8080/etl/v1/data_download?method=download"
response = requests.get(url)

with open("file.zip", "wb") as f:
    f.write(response.content)

with zipfile.ZipFile("file.zip", "r") as z:
    z.extractall("extracted_content")

view_list = []

# List the contents of the extracted directory
for filename in os.listdir("extracted_content"):
    with open(f"extracted_content/{filename}", "r") as f:
        view_list.append(json.load(f))

# Retrieving the list of existing buckets
s3 = boto3.client('s3')
response = s3.list_buckets()
print('Existing buckets:')
for bucket in response['Buckets']:
    bucket_name = bucket["Name"]
    print(f'  {bucket_name}')

# Uploading the file to S3
fol_path = 'extracted_content'
for filename in os.listdir("extracted_content"):
    path = f'{fol_path}/{filename}'
    with open(path, "rb") as f:
        s3.upload_fileobj(f, bucket_name, filename)

print("Files uploaded successfully")


# Retrieve the list of existing buckets
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

table = dynamodb.Table('jsonData')
print("Table object: {}".format(table))

for file in file_names:
    s3_object = s3.Object(bucket_name, file)
    json_data = json.loads(s3_object.get()['Body'].read().decode('utf-8'))

    data = json.dumps(json_data, separators=(',', ':'), default=str)
    json_dict = json.loads(data)
    string_data = convert_to_string(json_dict)

    table.put_item(
        Item=string_data
    )
    print(f'success - {file}')

print("Data moved into DynamoDB successfully")
