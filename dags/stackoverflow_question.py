from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task, task_group
from airflow.exceptions import (
    AirflowException,
    AirflowBadRequest,
    AirflowNotFoundException,
    AirflowFailException
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timezone

import requests
import logging
import json
import os


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=1)
def extract(kind, url, params):
    from plugins.stackoverflow_api import get_request

    response = get_request(kind=kind, url=url, params=params)      
    if isinstance(response, dict):
        return response.get('items', [])
    elif isinstance(response, list):
        return response
    else:
        logging.error("올바르지 않은 응답 형식")
        return []


@task
def transform(kind, columns, response, repo_id):
    from plugins.common import deep_get
    logging.info(f"[{kind}] 데이터 변환 시작")
    transformed_data = []
    for res in response:
        transformed_item = {}
        for new_col, old_col in columns.items():
            value = deep_get(res, old_col)
            transformed_item[new_col] = value 
        transformed_item['REPO_ID'] = repo_id    
        transformed_data.append(transformed_item)    
    return transformed_data


@task
def load(kind, content, repo=None):
    from plugins.awsfunc import awsfunc
    from plugins.file_ops import load_as_json

    logging.info(f"[{kind}] 데이터 저장 시작")
    dag_root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_as_json(dag_root_path, kind, content, repo)
    content = json.dumps(content)
    Bucket = 'de-2-2'
    save_path = 'raw/stackoverflow/question_list/'
    utc_now = datetime.now(timezone.utc)
    time_str = utc_now.strftime("%Y-%m-%d_%H-%M-%S")
    date_str = utc_now.strftime("%Y/%m/%d")
    
    file_name = save_path + f"{date_str}/{time_str}.json"
    s3_instance = awsfunc('s3')
    s3_instance.ec2tos3(content, Bucket, file_name)

@task
def check(kind, data_list: list, valid_check: dict):
    from plugins.common import check_data
    
    logging.info(f"[{kind}] 데이터 값 검증 시작")
    data = check_data(kind=kind, data_list=data_list, valid_check=valid_check)
    checked_data = data.get('checked_data', [])
    error_data = data.get('error_data', [])
    if len(error_data) > 0:
        logging.info(f"{error_data}")
        return checked_data
    else:
        return checked_data

@task
def remove_duplicates(content):
    unique_items = []
    item_keys = set()

    for value in content.values():
        for item in value:
            item_id = item["NODE_ID"]
            if item_id not in item_keys:
                unique_items.append(item)
                item_keys.add(item_id)
    return unique_items


@task_group
def getData(repo_fullnm, repo_id):
    full_nm = repo_fullnm.split('/')
    repo_nm = repo_fullnm[1]
    tasks = {'question': {'url': 'https://api.stackexchange.com/search', 'params': {'intitle':repo_nm,'order': 'desc', 'sort': 'relevance', 'page':'1', 'pagesize':'100', 'site':'stackoverflow', 'key': Variable.get('stackoverflow_token')},'columns': {'ID': 'question_id', 'TITLE': 'title', 'USER_ID': 'owner.user_id', 'USER_NM': 'owner.display_name', 'URL': 'link', 'CREATED_AT': 'creation_date', 'UPDATED_AT': 'last_edit_date'}, 'check': {'ID': {'type': 'int', 'nullable': False}, 'TITLE': {'type': 'string', 'nullable': False}, 'USER_ID': {'type': 'string', 'nullable': False}, 'USER_NM': {'type': 'integer', 'nullable': False}, 'URL': {'type': 'integer', 'nullable': False}, 'CREATED_AT': {'type': 'integer', 'nullable': False}, 'UPDATED_AT': {'type': 'integer', 'nullable': True}}}}
    result = {}
    for kind, task in tasks.items():

        url = task['url']

        try:
            if kind.startswith('language'):
                data = extract(kind=kind, url=url, params=task.get('params'))
            elif 'columns' in task:
                data = transform(kind=kind, columns=task.get('columns'), response=extract(kind=kind, url=url, params=task.get('params')), repo_id=repo_id)
            else:
                data = extract(kind=kind, url=url, params=task.get('params')) 
            if 'check' in task:
                result[kind] = check(kind=kind, data_list=data, valid_check=task['check'])
            else:
                result[kind] = data           
        except Exception as e:
            logging.error(f"[{kind}] 데이터 수집 실패\\n" + repr(e))
            result[kind] = None
    
    return result
    


with DAG(
    dag_id="stackoverflow_question_list",
    start_date=datetime(2023, 8, 29),
    schedule='@once',
    catchup=False,
    # on_success_callback=send_slack_message().success_alert,
    # on_failure_callback=send_slack_message().fail_alert,
    concurrency = 90, # 최대 n개의 task 동시 실행
    max_active_runs = 3, # 최대 n개 dag run 동시 실행

) as dag:
    
    from plugins.redis_api import get_all_repo_data_from_redis
    all_data = {}

    repositories = get_all_repo_data_from_redis()
    for repo_info in repositories.values():
        repo_id = repo_info.get("ID")
        data = getData(repo_info.get("FULL_NM"), repo_id=repo_id)
        if data:
            all_data[repo_id] = data

    load(kind="question", content=all_data)
       