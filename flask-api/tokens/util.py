import requests
import redis
import json
import boto3
from botocore.exceptions import ClientError
import logging
import os

redis = redis.StrictRedis(host='redis', port=6379, db=5, decode_responses=True)

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY") 
AWS_SECRET_REGION = os.environ.get("AWS_SECRET_REGION") 
TOKEN_NAME = os.environ.get("TOKEN_NAME")

def get_aws_secret(secret_name: str) -> dict:
    """
    # AWS Secret Manager에서 값을 가져옴
    :param secret_name: 시크릿 이름
    :param secret_key: 시크릿에서 가져올 키
    :return: 시크릿 값 (없을 시 None 반환)
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        client = session.client(
            service_name='secretsmanager',
            region_name=AWS_SECRET_REGION
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_str = get_secret_value_response['SecretString']
        if secret_str:
            secret_data = json.loads(secret_str)
            return secret_data
        else:
            logging.info("SecretString not found in the response.")
            return {}
    except ClientError as e:
        logging.error(f"Error retrieving secret: {e}") # 로깅
        return None

def update_tokens_usage(tokens: dict, is_update:bool):
    """
    # 토큰의 사용량을 가져와 Redis에 처음 저장

    :param tokens: 토큰 정보가 담긴 딕셔너리
    """
    for token_key, token_value in tokens.items():
        token_usage = get_token_usage(token_value)
        logging.info(f"조회 완료: {token_key} {token_usage}")
        if token_usage:
            save_tokens_info(token_key, token_value, token_usage, is_update)
        else:
            logging.error(f"Failed to fetch usage for token: {token_key}")

def get_token_usage(token_value: str):
    """
    # 토큰의 사용량 정보를 가져옴
    https://docs.github.com/en/free-pro-team@latest/rest/rate-limit/rate-limit?apiVersion=2022-11-28#get-rate-limit-status-for-the-authenticated-user
    
    :param token_value: API 토큰 값
    :return: 사용량 정보 딕셔너리 또는 None (에러 시)
    """
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    try:
        response = requests.get("https://api.github.com/rate_limit", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch rate limit for token: {token_value}. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while fetching rate limit for token: {token_value}\nError: {e}")
    return None

def save_tokens_info(token_key: str, token_value: str, token_usage, is_update:bool=False):
    """
    # 토큰의 사용량 정보를 Redis에 저장.
    
    :param token_key: 토큰의 키
    :param token_value: 토큰의 값
    :param token_usage: 사용량 정보 딕셔너리(json)
    """
    redis.hset(token_key, "token_key", token_key)
    redis.hset(token_key, "token_value", token_value)
    redis.hset(token_key, "total_limit", token_usage.get("rate").get("limit"))
    redis.hset(token_key, "total_remaining", token_usage.get("rate").get("remaining"))
    redis.hset(token_key, "total_reset", token_usage.get("rate").get("reset"))
    redis.hset(token_key, "core_limit", token_usage.get("resources").get("core").get("limit"))
    redis.hset(token_key, "core_remaining", token_usage.get("resources").get("core").get("remaining"))
    redis.hset(token_key, "core_reset", token_usage.get("resources").get("core").get("reset"))
    redis.hset(token_key, "search_limit", token_usage.get("resources").get("search").get("limit"))
    redis.hset(token_key, "search_remaining", token_usage.get("resources").get("search").get("remaining"))
    redis.hset(token_key, "search_reset", token_usage.get("resources").get("search").get("reset"))
    if is_update == False:
        redis.hset(token_key, "allocated", "false")
