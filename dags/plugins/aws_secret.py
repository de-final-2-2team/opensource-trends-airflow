import boto3
from botocore.exceptions import ClientError
from env.aws_credentials import AWS_ACCESS_KEY_ID ,AWS_SECRET_ACCESS_KEY, AWS_SECRET_REGION
import json

def get_aws_secret(secret_name):
    session = boto3.session.Session(
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    client = session.client(
        service_name='secretsmanager',
        region_name=AWS_SECRET_REGION
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    secret_str = get_secret_value_response['SecretString']
    secret = json.loads(secret_str)

    return secret

