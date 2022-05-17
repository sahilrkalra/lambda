import json, os
import unittest
import boto3
import sys
from moto import mock_s3

sys.path.append('../../main/python/lambda_function')
from lambda_function import get_data_from_file
from lambda_function import lambda_handler

S3_BUCKET_NAME = 'usertransactionssource'
DEFAULT_REGION = 'us-east-1'

S3_TEST_FILE_KEY = 'users.json'
S3_TEST_FILE_CONTENT = [
    {"User1": "John"},
    {"User2": "Erick"}
]

@mock_s3
class TestLambdaFunction(unittest.TestCase):

    def setUp(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = DEFAULT_REGION
        self.S3_CLIENT = boto3.client("s3", region_name=DEFAULT_REGION)
        self.s3_bucket = self.S3_CLIENT.create_bucket(Bucket=S3_BUCKET_NAME)
        self.S3_CLIENT.put_object(Bucket=S3_BUCKET_NAME, Key=S3_TEST_FILE_KEY, Body=json.dumps(S3_TEST_FILE_CONTENT))


    def test_get_data_from_file(self):
        file_content = get_data_from_file(S3_BUCKET_NAME, S3_TEST_FILE_KEY)
        self.assertEqual(file_content, S3_TEST_FILE_CONTENT)


    def test_lambda_handler(self):
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {
                            'name': S3_BUCKET_NAME
                        },
                        'object': {
                            'key': S3_TEST_FILE_KEY
                        }
                    }
                }
            ]
        }

        result = lambda_handler(event, {})
        self.assertEqual(result, {'StatusCode': 200, 'Message': [S3_TEST_FILE_CONTENT]})


if __name__ == '__main__':
    unittest.main()