# -*- coding: utf-8 -*-
import json
import boto3
import botocore
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError


class awsfunc:
    def __init__(self, service_name):
        # Boto3 클라이언트 생성 (IAM 역할을 사용)
        session = boto3.session.Session()
        self.client = session.client(
            service_name=service_name,
            region_name="us-east-1"
        )

    def getapikey(self, secret_id):
        # Secret ID 설정
        secret_id = secret_id

        try:
            # Secret 값을 가져오기
            response = self.client.get_secret_value(SecretId=secret_id)

            # Secret 값 추출 및 처리
            if 'SecretString' in response:
                secret_string = response['SecretString']
                secret_data = json.loads(secret_string)
                api_key = secret_data.get(secret_id)  # 실제 Secret 내용에서 필요한 값 추출
                print(secret_id + "access key found")
                return api_key
            else:
                print("No SecretString found.")

        except NoCredentialsError:
            print("No AWS credentials found.")
    
    def ec2tos3(self, Body, Bucket, Key):
        # ec2에서 추출한 데이터 s3로 write

        encoded_data = Body.encode('utf-8')
        self.client.put_object(Body=encoded_data, Bucket=Bucket, Key=Key)

    def read_json_from_s3(self, Bucket, Path):
        response = self.client.get_object(Bucket=Bucket, Key=Path)
        content = response["Body"]
        jsonObject = json.loads(content.read().strip())
        return jsonObject
    
    def get_file_name_from_s3(self, Bucket, Path):
        objects = self.client.list_objects(Bucket=Bucket, Prefix = Path)
        contents = objects.get('Contents', [])
        last_content = contents[-1]
        file_path = last_content['Key']
        return file_path
            
            

