from airflow.models import Variable
from airflow.exceptions import (
    AirflowException,
    AirflowBadRequest,
    AirflowNotFoundException,
    AirflowFailException
)
import requests
import logging


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
        logging.info(f"[{kind}] 데이터 수집 시작")
        headers = get_header()
        response = requests.get(url=url, 
                                headers=headers,
                                params=params)
        # 200이 아닌 경우 에러 발생
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            raise AirflowBadRequest(f"[{kind}] 데이터 수집 실패\\n" + response.text)
        elif response.status_code == 401:
            raise AirflowException(f"[{kind}] 데이터 수집 실패\\n" + response.text)
        elif response.status_code == 404:
            raise AirflowNotFoundException(f"[{kind}] 데이터 수집 실패\\n" + response.text)
        else:
            raise AirflowFailException(f"[{kind}] 데이터 수집 실패\\n" + response.text)
    except Exception as e:
        raise AirflowFailException(f"[{kind}] 데이터 수집 실패\\n" + repr(e))