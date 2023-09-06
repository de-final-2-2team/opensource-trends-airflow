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






def get_request(kind, url, params):
    try:
        logging.info(f"[{kind}] 데이터 수집 시작 : {url}")
        response = requests.get(url=url, 
                                params=params, timeout=30)
        # 200이 아닌 경우 에러 발생
        if response.status_code == 200:
            try:
                response_json = response.json()
                return response_json
            except json.JSONDecodeError:
                logging.error(f"JSON 디코딩 오류: {response.text}")
                return None
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