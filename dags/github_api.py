from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import (
    AirflowException,
    AirflowBadRequest,
    AirflowNotFoundException,
    AirflowFailException
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


request_list = {
    "lincense": {
        "url": "https://api.github.com/licenses",
        "params": {"per_page": 30, "page": 1}
    },
    "commit_per_hour": {
        "name": "시간 당 commit 수",
        "url": "https://api.github.com/repos/{OWNER}/{REPO}/stats/punch_card"
    },
    "release": {
        "url": "https://api.github.com/repos/{OWNER}/{REPO}/tags",
        "params": {"per_page": 30, "page": 1}
    },
    "project": {
        "url": "  https://api.github.com/repos/{OWNER}/{REPO}/projects",
        "params": {"state": "all", "per_page": 30, "page": 1}
    },
    "contributor_activity": {
        "url": "https://api.github.com/repos/{OWNER}/{REPO}/stats/code_frequency"
    }, 
    "community_profile": {
        "url": "https://api.github.com/repos/geekan/{OWNER}/{REPO}/profile"
    },
    "language": {
        "url": "https://api.github.com/repos/{OWNER}/{REPO}/languages"
    }
}


def get_header():
    token = Variable.get("github_token")
    api_version = Variable.get("git_api_version")
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": api_version
    }


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=1)
def extract(kind):
    try:
        logging.info(f"[{kind}] 데이터 수집 시작")
        headers = get_header()
        url = request_list.get(kind, {}).get('url')
        params= request_list.get(kind, {}).get('params')
        response = requests.get(url=url, 
                                headers=headers,
                                params=params)
        # 200이 아닌 경우 에러 발생
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            raise AirflowBadRequest(f"[{kind}] 데이터 수집 실패\\n" + response.content)
        elif response.status_code == 401:
            raise AirflowException(f"[{kind}] 데이터 수집 실패\\n" + response.content)
        elif response.status_code == 404:
            return AirflowNotFoundException(f"[{kind}] 데이터 수집 실패\\n" + response.content)
        else:
            raise AirflowFailException(f"[{kind}] 데이터 수집 실패\\n" + response.content)
    except Exception as e:
        logging.error(f"[{kind}] 데이터 수집 실패\\n" + repr(e))
        raise e


@task()
def load_as_json(kind, content):
    with open(f"./{kind}.json", 'w') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{kind}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{kind}] 데이터 수집 완료")


"""
DAG 스케줄 간격 설정
1. 매크로 프리셋 활용
    schedule_interval = '@daily'
2. cron 활용
    schedule = 'miniute(0-59) hour(0-23) day_of_month(1-31) month(1-12) day_of_week(0-6, 0:sunday)'
3. timedelta 활용
    schedule_interval = timedelta(days=3)

DAG 설정
참고: https://airflow.apache.org/docs/apache-airflow/2.2.3/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
1. catchup
    - True(default): 이전 DAG 종료된 이후 다음 DAG를 실행
    - False: 이전 DAG가 실행 중이더라도 예정대로 다음 DAG를 실
"""
with DAG(
    dag_id='meta',
    start_date=datetime(2023, 8, 15, 21),  # 날짜가 미래인 경우 실행이 안됨
    schedule='*/10 * * * *',
    max_active_runs=1,
    catchup=False,
) as dag:
    response = extract('lincense')
    load_as_json('lincense', response)