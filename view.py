import boto3
import csv
import json
import codecs


# declare S3 variables and read the CSV content from S3 bucket.
targetBucket = 'tien-duong1151'
csvFile = 'data/coins_data.csv'
jsonFile = 'data/coins_data.json'
# connect to S3 using boto3 client    
s3_client = boto3.client(service_name='s3')
# get S3 object
result = s3_client.get_object(Bucket=targetBucket, Key=csvFile) 
csv_content = result['Body'].read().splitlines()
# use CSV reader to read the object and decode the contents.
read = csv.reader(codecs.iterdecode(csv_content, 'utf-8'))
# convert from CSV to JSON format.
line = []
for x in read:
    test1 = str(x[0])
    test2 = str(x[1])
    test3 = str(x[2])
    y = '{ "test1": ' + '"' + test1 + '"' + ','  \
        + ' "test2": ' + '"' + test2 + '"' + ',' \
        + ' "test3": ' + '"' +  test3 + '"' + '}'
    line.append(y)
# put back the JSON file to S3 bucket. 
s3_client.put_object(
    	Bucket=targetBucket,
    	Body= str(line).replace("'",""),
    	Key=jsonFile,
    	ServerSideEncryption='AES256')