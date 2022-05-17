import json
import os
import logging
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_data_from_file(bucket, key):
    '''
    Function reads json file uploaded to S3 bucket
    '''

    S3_CLIENT = boto3.client('s3')
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)

    content = response['Body']
    data = json.loads(content.read())

    LOGGER.info(f'{bucket}/{key} file content: {data}')

    return data


def lambda_handler(event, context):
    '''
    Main Lambda function method
    '''
    LOGGER.info(f'Event structure: {event}')
    s3_files_list = list()

    try:
        for record in event['Records']:
            s3_bucket = record['s3']['bucket']['name']
            s3_file = record['s3']['object']['key']
            data = get_data_from_file(s3_bucket, s3_file)
            s3_files_list.append(data)
    
        output = {
            'StatusCode': 200,
            'Message': s3_files_list
        }
    except Exception as e:
        print(f'Error {str(e)}')
        output = {
            'StatusCode': -1,
            'Message': f'Something went wrong while processing {s3_file}'
        }
    
    LOGGER.info(f'Response {output}')
    return output