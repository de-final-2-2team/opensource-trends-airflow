from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.macros import timedelta
from airflow.decorators import task, task_group
from airflow.operators.python_operator import PythonOperator
import logging

SNOWFLAKE_CONN_ID = 'snowflake_dev'
DATABASE = "dev"
SCHEMA = "raw_data"
S3_BUCKET_NAME = 'de-2-2'
git_table = {
    'repository_list' : 'REPOSITORY_TB',
    'release_tag_list' : 'RELEASE_TB',
    'project_list' : 'PROJECT_TB',
    'pr_list' : 'PR_TB',
    'license_list' : 'LICENSE_TB',
    'language_list' : 'LANGUAGE_TB',
    'issue_list' : 'ISSUE_TB',
    'fork_list' : 'FORK_TB',
    'commit_list' : 'COMMIT_TB',
    'commit_activity' : 'CM_ACT_TB', 
}

def get_s3_and_upsert(exec_time):
    exec_time = exec_time.replace("-", "/")
    logging.info(f"@@ exec_time {exec_time}")

    result = {
        'REPOSITORY_TB': [],
        'RELEASE_TB': [],
        'PROJECT_TB': [],
        'PR_TB': [],
        'LICENSE_TB': [],
        'LANGUAGE_TB': [],
        'ISSUE_TB': [],
        'FORK_TB': [],
        'COMMIT_TB': [],
        'CM_ACT_TB': []
    }

    for s3_prefix, tb_table in git_table.items():
        # S3 버킷에서 파일 목록 가져오기
        s3_key_prefix = str(f'analytics/github/{s3_prefix}/{exec_time}/')
        s3_objects = awsfunc('s3').get_file_list_from_s3(Bucket=S3_BUCKET_NAME, Path=s3_key_prefix)

        # logging.info(f"s3_prefix : {s3_prefix}")
        # logging.info(f"s3 s3_objects : {s3_objects}")
        for s3_object in s3_objects:
            filename = s3_object['Key']
            logging.info(f"# !! s3 filename : {filename}")
            result[tb_table].append(filename) 
    return result


def s3ToSnow(**kwargs):
    file_list = kwargs['ti'].xcom_pull(task_ids='get_s3_files')
    import string
    import random
    from airflow.models import Variable


    logging.info(f"# !! s3 file_list : {file_list}") # 확인 완료 
    aws_access_key_id = Variable.get('aws_access_key_id').strip('() ')
    aws_secret_access_key = Variable.get('aws_secret_access_key').strip('() ')

    for tb_table, filename in file_list.items():
        if tb_table== "REPOSITORY_TB":
            sql = upsert_sql.REPOSITORY_TB
        elif tb_table == "RELEASE_TB" :
            sql = upsert_sql.RELEASE_TB
        elif tb_table == "PROJECT_TB" :
            sql = upsert_sql.PROJECT_TB
        elif tb_table == "PR_TB" :
            sql = upsert_sql.PR_TB
        elif tb_table == "LICENSE_TB" :
            sql = upsert_sql.LICENSE_TB
        elif tb_table == "LANGUAGE_TB" :
            sql = upsert_sql.LANGUAGE_TB
        elif tb_table == "ISSUE_TB" :
            sql = upsert_sql.ISSUE_TB
        elif tb_table == "FORK_TB" :
            sql = upsert_sql.FORK_TB
        elif tb_table == "COMMIT_TB" :
            sql = upsert_sql.COMMIT_TB
        elif tb_table == "CM_ACT_TB" :
            sql = upsert_sql.CM_ACT_TB
        else:
            sql = upsert_sql.REPOSITORY_TB

        letters_set = string.ascii_letters
        random_list = random.sample(letters_set,5)
        task_id = ''.join(random_list)

        create_temp = SnowflakeOperator(  # 자체가 실행이 안됨 (아마 task안에 task 중첩이라 그런것 같다.)
            task_id=f"upsert_{tb_table}_{task_id}",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            database=DATABASE,
            schema=SCHEMA,
            sql=sql,
            params={
                "table": f"temp_{tb_table}",
                "bucket": S3_BUCKET_NAME, 
                "filename": filename,
                # "filename" :"analytics/github/repository_list/2023/08/30/part-00000-4f24f792-042b-4673-8e80-6bdb68532d01-c000.snappy.parquet",
                "AWS_KEY_ID" :  aws_access_key_id, 
                "AWS_SECRET_KEY" : aws_secret_access_key
            })
        create_temp

with DAG(
    dag_id="s3_to_snowflake",
    start_date=datetime(2023, 8, 29), # 데이터 수집 시작 + 1일
    # schedule="15 0 * * *",  # 매일 자정 15분
    schedule=None,
    catchup=False,
    # catchup=True,
    max_active_runs = 1,
    max_active_tasks = 1
) as dag:
    
    table_prefix = 'repository_list'
    from plugins.awsfunc import awsfunc 
    import sql.upsert_sql as upsert_sql


    get_s3_files = PythonOperator(
        task_id='get_s3_files',
        python_callable=get_s3_and_upsert,
        op_kwargs={
            "exec_time": "{{ ds }}"
        },dag=dag
    )

    s3ToSnow = PythonOperator(
        task_id='s3ToSnow',
        python_callable=s3ToSnow,
        provide_context=True,
    )
    get_s3_files >> s3ToSnow