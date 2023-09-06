from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging



SNOWFLAKE_CONN_ID = 'snowflake_dev'
DATABASE = "dev"
SCHEMA = "raw_data"
S3_BUCKET_NAME = 'de-2-2'
git_table = {
    'repository_list': 'REPOSITORY_TB',
    'release_tag_list': 'RELEASE_TB',
    'project_list': 'PROJECT_TB',
    'pr_list': 'PR_TB',
    'license_list': 'LICENSE_TB',
    'issue_list': 'ISSUE_TB',
    'fork_list': 'FORK_TB',
    'commit_list': 'COMMIT_TB',
    'commit_activity': 'CM_ACT_TB',
    'language_list': 'LANGUAGE_TB',
}

def get_s3_files(exec_time, **kwargs):
    execution_date = kwargs['execution_date']
    before_one_day = execution_date - timedelta(days=1)
    exec_time = before_one_day.strftime("%Y/%m/%d")
    logging.info(f"@@ exec_time {exec_time}")

    result = {
        'REPOSITORY_TB': [],
        'RELEASE_TB': [],
        'PROJECT_TB': [],
        'PR_TB': [],
        'LICENSE_TB': [],
        'ISSUE_TB': [],
        'FORK_TB': [],
        'COMMIT_TB': [],
        'CM_ACT_TB': [],
        'LANGUAGE_TB': [],
    }

    for s3_prefix, tb_table in git_table.items():
        s3_key_prefix = str(f'analytics/github/{s3_prefix}/{exec_time}/')
        s3_objects = awsfunc('s3').get_file_list_from_s3(Bucket=S3_BUCKET_NAME, Path=s3_key_prefix)

        for s3_object in s3_objects:
            filename = s3_object['Key']
            result[tb_table].append(filename)
    return result


def s3ToSnow(file_list, **kwargs):
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
    from airflow.models import Variable
    from jinja2 import Template
    import string
    import random

    aws_access_key_id = Variable.get('aws_access_key_id').strip('() ')
    aws_secret_access_key = Variable.get('aws_secret_access_key').strip('() ')

    for tb_table, filenames in file_list.items():
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

        for filename in filenames:
            letters_set = string.ascii_letters
            random_list = random.sample(letters_set, 5)
            task_id = ''.join(random_list)
            
            params = {
                "table": f"temp_{tb_table}",
                "bucket": S3_BUCKET_NAME,
                "filename": filename,
                "AWS_KEY_ID": aws_access_key_id,
                "AWS_SECRET_KEY": aws_secret_access_key
            }
            template = Template(sql)
            sql_rendered = template.render(params=params)

            snowflake_op  = SnowflakeOperator(
                task_id=f"upsert_{tb_table}_{task_id}",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                database=DATABASE,
                schema=SCHEMA,
                sql=sql_rendered,
                params=params,
                dag=kwargs['dag']
            )
            snowflake_op.execute(context={})



with DAG(
    dag_id="s3_to_snowflake",
    start_date=datetime(2023, 8, 28),
    catchup=True,
    schedule="15 0 * * *",  # 매일 00:15분에 그 전날의 데이터를 Upsert
    max_active_runs= 1,
    max_active_tasks= 1
) as dag:

    table_prefix = 'repository_list'
    from plugins.awsfunc import awsfunc
    import sql.upsert_sql as upsert_sql

    get_s3_files_task = PythonOperator(
        task_id='get_s3_files',
        python_callable=get_s3_files,
        op_args=["{{ ds }}"],
        provide_context=True
    )

    s3_to_snow_task = PythonOperator(
        task_id='s3_to_snow',
        python_callable=s3ToSnow,
        op_args=[get_s3_files_task.output],
        provide_context=True
    )

    get_s3_files_task >> s3_to_snow_task