from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timezone
from airflow.decorators import task, task_group
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
import logging


def send_slack_message():
    from plugins.awsfunc import awsfunc
    from plugins.slack import SlackAlert

    s3_handler = awsfunc('secretsmanager')
    slack_token = s3_handler.getapikey(secret_id="slack-token")
    slack_alert = SlackAlert(channel="#monitoring_airflow", token=slack_token)
    return slack_alert

with DAG(
    dag_id="github_repo_v2",
    start_date=datetime(2023, 8, 29),
    schedule='50 23,11 * * *',
    catchup=False,
    on_success_callback=send_slack_message().success_alert,
    on_failure_callback=send_slack_message().fail_alert,
    concurrency = 90, # 해당 DAG에 대해 최대 동시에 실행 가능한 Task 수
    max_active_runs = 3  #  해당 DAG에 대해 동시에 RUN 가능한 DAG 수 
) as dag:

    @task
    def _delete_all_repo_from_redis():
        from plugins.redis_api import delete_all_repo_from_redis
        
        try:
            delete_all_repo_from_redis()
        except Exception as e:
            logging.error(f"레파지토리 정보 Redis 삭제 실패 {e}")


    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=1, pool="api_pool")
    def extract(kind, url, params=None):
        from plugins.github_api import get_request

        response = get_request(kind=kind, url=url, params=params)      
        if isinstance(response, dict):
            if kind.startswith('language'): # info의 language
                response = [{"LANG_NM": key, "LANG_BYTE": value} for key, value in response.items()]
                return response
            else:
                return response.get('items', [])
        elif isinstance(response, list):
            return response
        else:
            logging.error("올바르지 않은 응답 형식")
            return []

    @task
    def transform(kind, columns, response):
        from plugins.common import deep_get
        
        logging.info(f"[repo:{kind}] 데이터 변환 시작")
        return [{new_col: deep_get(item, old_col) for new_col, old_col in columns.items()} for item in response]

    @task
    def check(kind, data_list: list, valid_check: dict):
        from plugins.common import check_data
        
        logging.info(f"[repo:{kind}] 데이터 값 검증 시작")
        data = check_data(kind=kind, data_list=data_list, valid_check=valid_check)
        checked_data = data.get('checked_data', [])
        error_data = data.get('error_data', [])
        if len(error_data) > 0:
            send_slack_message().fail_alert(context=error_data)
            return checked_data
        else:
            return checked_data

    @task
    def load(kind, content, repo=None):
        from plugins.awsfunc import awsfunc
        import json

        utc_now = datetime.now(timezone.utc)
        time_str = utc_now.strftime("%Y-%m-%d_%H-%M-%S")
        date_str = utc_now.strftime("%Y/%m/%d")

        s3_handler = awsfunc('s3')
        bucket_name = "de-2-2"
        file_key = f"raw/github/repo/{date_str}/{time_str}.json"

        logging.info(f"[repo:{kind}] 데이터 저장 시작")
        s3_handler.ec2tos3(json.dumps(content), bucket_name, file_key)

    @task 
    def _save_repo_to_redis(content):
        from plugins.redis_api import save_repo_to_redis

        try:
            for repo in content:
                _id = repo.get('ID')
                full_nm = repo.get('FULL_NM')
                if _id and full_nm:
                    save_repo_to_redis(_id, full_nm)
                else:
                    logging.error("레파지토리 ID 또는 FULL_NM 누락")
        except Exception as e:
            logging.error(f"레파지토리 정보 Redis 저장 실패 {e}")

    @task
    def remove_duplicates(content):
        unique_items = []
        item_keys = set()

        for value in content.values():
            for item in value:
                item_id = item['ID']
                if item_id not in item_keys:
                    unique_items.append(item)
                    item_keys.add(item_id)
        return unique_items

    @task_group
    def getData():
        result = {}
        tasks = {'repo_stars': {'url': 'https://api.github.com/search/repositories?q={Q}', 'params': {'sort': 'stars', 'per_page': 100, 'page': 1}, 'columns': {'ID': 'node_id', 'REPO_NM': 'name', 'FULL_NM': 'full_name', 'OWNER_ID': 'owner.node_id', 'OWNER_NM': 'owner.login', 'CREATED_AT': 'created_at', 'UPDATED_AT': 'updated_at', 'PUSHED_AT': 'pushed_at', 'STARGAZERS_CNT': 'stargazers_count', 'WATCHERS_CNT': 'watchers_count', 'LANG_NM': 'language', 'FORKS_CNT': 'forks_count', 'OPEN_ISSUE_CNT': 'open_issues_count', 'SCORE': 'score', 'LIC_ID': 'license.node_id'}, 'check': {'ID': {'type': 'string', 'nullable': False}, 'REPO_NM': {'type': 'string', 'nullable': False}, 'FULL_NM': {'type': 'string', 'nullable': False}, 'OWNER_ID': {'type': 'string', 'nullable': False}, 'OWNER_NM': {'type': 'string', 'nullable': True}, 'CREATED_AT': {'type': 'string', 'nullable': False}, 'UPDATED_AT': {'type': 'string', 'nullable': False}, 'PUSHED_AT': {'type': 'string', 'nullable': False}, 'STARGAZERS_CNT': {'type': 'integer', 'nullable': False}, 'WATCHERS_CNT': {'type': 'string', 'nullable': False}, 'LANG_NM': {'type': 'string', 'nullable': True}, 'FORKS_CNT': {'type': 'interger', 'nullable': False}, 'OPEN_ISSUE_CNT': {'type': 'interger', 'nullable': False}, 'SCORE': {'type': 'interger', 'nullable': False}, 'LIC_ID': {'type': 'string', 'nullable': True}}}, 'repo_forks': {'url': 'https://api.github.com/search/repositories?q={Q}', 'params': {'sort': 'forks', 'per_page': 100, 'page': 1}, 'columns': {'ID': 'node_id', 'REPO_NM': 'name', 'FULL_NM': 'full_name', 'OWNER_ID': 'owner.node_id', 'OWNER_NM': 'owner.login', 'CREATED_AT': 'created_at', 'UPDATED_AT': 'updated_at', 'PUSHED_AT': 'pushed_at', 'STARGAZERS_CNT': 'stargazers_count', 'WATCHERS_CNT': 'watchers_count', 'LANG_NM': 'language', 'FORKS_CNT': 'forks_count', 'OPEN_ISSUE_CNT': 'open_issues_count', 'SCORE': 'score', 'LIC_ID': 'license.node_id'}, 'check': {'ID': {'type': 'string', 'nullable': False}, 'REPO_NM': {'type': 'string', 'nullable': False}, 'FULL_NM': {'type': 'string', 'nullable': False}, 'OWNER_ID': {'type': 'string', 'nullable': False}, 'OWNER_NM': {'type': 'string', 'nullable': True}, 'CREATED_AT': {'type': 'string', 'nullable': False}, 'UPDATED_AT': {'type': 'string', 'nullable': False}, 'PUSHED_AT': {'type': 'string', 'nullable': False}, 'STARGAZERS_CNT': {'type': 'integer', 'nullable': False}, 'WATCHERS_CNT': {'type': 'string', 'nullable': False}, 'LANG_NM': {'type': 'string', 'nullable': True}, 'FORKS_CNT': {'type': 'interger', 'nullable': False}, 'OPEN_ISSUE_CNT': {'type': 'interger', 'nullable': False}, 'SCORE': {'type': 'interger', 'nullable': False}, 'LIC_ID': {'type': 'string', 'nullable': True}}}}
        for kind, task in tasks.items():
            url = task['url']
            try:
                if kind.startswith('language'):
                    data = extract(kind=kind, url=url, params=task.get('params'))
                elif 'columns' in task:
                    data = transform(kind=kind, columns=task.get('columns'), response=extract(kind=kind, url=url, params=task.get('params')))
                else:
                    data = extract(kind=kind, url=url, params=task.get('params'))
                if 'check' in task:
                    result[kind] = check(kind=kind, data_list=data, valid_check=task['check'])
                else:
                    result[kind] = data
            except Exception as e:
                logging.error(f"[repo:{kind}] 데이터 수집 실패\\n" + repr(e))
                result[kind] = None
        deduplicated_data = remove_duplicates(result)

        delete_repo_redis_task = _delete_all_repo_from_redis()
        deduplicated_data >> delete_repo_redis_task 
        delete_repo_redis_task >> _save_repo_to_redis(deduplicated_data)

        return deduplicated_data


    load(kind="repo", content=getData())
