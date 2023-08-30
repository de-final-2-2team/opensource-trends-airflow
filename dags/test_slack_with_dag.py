from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.slack import SlackAlert
from plugins.aws_secret import get_aws_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'retries': 1,
}

dag = DAG(
    dag_id = 'slack_alert_test',
    default_args = default_args,
    schedule_interval=None,
    catchup=False
)

slack_token = get_aws_secret(secret_name = "slack-token")['slack-token']
slack_alert = SlackAlert(channel="#monitoring_airflow", token=slack_token)

def print_hello():
    print("hello!")
    return "hello!"

print_hello = PythonOperator(
    task_id = 'print_hello',
    python_callable = print_hello,
    dag = dag,
    on_success_callback=slack_alert.success_alert,
    on_failure_callback=slack_alert.fail_alert
    )
