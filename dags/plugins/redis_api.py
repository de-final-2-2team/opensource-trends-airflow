import requests
from airflow.exceptions import (
    AirflowException,
    AirflowBadRequest,
    AirflowNotFoundException,
    AirflowFailException
)
import logging
import json


def request_api(url, method='GET', json_data=None, timeout=15):
    """
    Flask API에 요청을 보내는 함수

    :param url: API URL
    :param method: HTTP 메서드 
    :param json_data: 전송할 JSON 데이터 
    :param timeout: 요청 타임아웃 시간 
    :return: 응답 데이터 (성공 시) 또는 예외 발생
    """
    try:
        response = requests.request(method, url, json=json_data, timeout=timeout)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            raise AirflowBadRequest(f"[{response.status_code}] API 요청실패 - {response.text}")
        elif response.status_code == 401:
            raise AirflowException(f"[{response.status_code}] API 요청실패 - {response.text}")
        elif response.status_code == 404:
            raise AirflowNotFoundException(f"[{response.status_code}] API 요청실패 - URL NOT FOUND")
        else:
            raise AirflowFailException(f"[{response.status_code}] API 요청실패 - {response.text}")
    except requests.exceptions.RequestException as req_exception:
        raise req_exception
    except ValueError as value_error:
        raise value_error
    except Exception as e:
        raise AirflowFailException(f"[{response.status_code}] API 요청실패 - {str(e)}")


def save_repo_to_redis(_id, full_nm):
    url = "http://flask-api:5000/repo/save_repo"
    repo_data = {
        "ID": _id,
        "FULL_NM": full_nm
    }
    response_data = request_api(url, method='POST', json_data=repo_data)
    logging.info(f"[결과 ] save_repo_to_redis {response_data}")
    return response_data


def get_repo_from_redis(repo_id):
    """
    # 특정 repo를 repo_id로 가져오는 함수

    응답 예시:
        {
        "ID": "string",
        "FULL_NM": "string"
        }
    """
    url = f"http://flask-api:5000/repo/{repo_id}"
    response_data = request_api(url)
    logging.info(f"[결과 get_repo_from_redis] {response_data}")
    return response_data


def get_all_repo_ids_from_redis():
    """
    # 모든 repo id를 가져오는 함수

    응답 예시: 
        [
        "MDEwOlJlcG9zaXRvcnk4MzgyMTY2OQ==",
        "MDEwOlJlcG9zaXRvcnk2MjcyMjkzNw==",
        "MDEwOlJlcG9zaXRvcnkxMDg1MTM0ODU=",
        "MDEwOlJlcG9zaXRvcnkzNDQwNzY1MQ==",
        ]
    """
    url = "http://flask-api:5000/repo/all_ids"
    response_data = request_api(url)
    logging.info(f"[결과 get_all_repo_ids_from_redis] {response_data}")
    return response_data


def get_all_repo_data_from_redis():
    """
    # 모든 repo 데이터를 가져오는 함수

    응답 예시:
    {
        "MDEwOlJlcG9zaXRvcnk4MzgyMTY2OQ==": {
            "ID": "MDEwOlJlcG9zaXRvcnk4MzgyMTY2OQ==",
            "FULL_NM": "Qiskit/qiskit"
        },
        "MDEwOlJlcG9zaXRvcnk2MjcyMjkzNw==": {
            "ID": "MDEwOlJlcG9zaXRvcn
            
            k2MjcyMjkzNw==",
            "FULL_NM": "tencentyun/qcloud-documents"
        },
    }
    """
    url = "http://flask-api:5000/repo/all_data"
    response_data = request_api(url)
    logging.info(f"[결과 get_all_repo_data_from_redis] {response_data}")
    # all_repo = json.loads(response_data)
    if isinstance(response_data, dict):
        all_repo = response_data
    else:
        all_repo = json.loads(response_data)
    return all_repo