from airflow.models import Variable
from airflow.exceptions import (
    AirflowException,
    AirflowBadRequest,
    AirflowNotFoundException,
    AirflowFailException
)
import requests
import logging
import json


def get_header():
    token = Variable.get("github_token")
    api_version = Variable.get("git_api_version")
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": api_version
    }


def get_request(kind, url, params=None):
    try:
        logging.info(f"[{kind}] 데이터 수집 시작 : {url}")
        headers = get_header()
        response = requests.get(url=url, 
                                headers=headers,
                                params=params, timeout=(10, 10))
        if response.status_code in [200, 201, 202, 204]:
            # logging.info(f"[{kind}] 데이터 수집 성공 { response.status_code} \\n")
            try:
                response_json = response.json()
                return response_json
            except json.JSONDecodeError:
                logging.error(f"JSON 디코딩 오류: {response.text}")
                return None
        elif response.status_code == 400:
            raise AirflowBadRequest(f"[{kind}] 데이터 수집 실패 400\\n" + response.text)
        elif response.status_code == 401:
            raise AirflowException(f"[{kind}] 데이터 수집 실패 401 \\n" + response.text)
        elif response.status_code == 404:
            raise AirflowNotFoundException(f"[{kind}] 데이터 수집 실패 404 \\n" + response.text)
        else:
            logging.error(f"[{kind}] 데이터 수집 실패 else { response.status_code} \\n" + response.text)
            raise AirflowFailException(f"[{kind}] 데이터 수집 실패 else \\n" + response.text)
    except Exception as e:
        raise AirflowFailException(f"[{kind}] 데이터 수집 실패 except\\n" + repr(e))