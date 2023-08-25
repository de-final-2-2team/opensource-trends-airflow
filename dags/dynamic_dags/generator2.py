from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import yaml
import os


@task
def generate_dags():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    dag_dir = os.path.dirname(file_dir)
    env = Environment(loader=FileSystemLoader(file_dir))
    template = env.get_template('googleTrend.jinja2')

    with open(f"{file_dir}/config/googletrend.yaml", "r") as cf:
        config = yaml.safe_load(cf)
        for dag_id in config['dag_ids']:
            info = config['dag_ids'][dag_id]
            with open(f"{dag_dir}/googletrend_{dag_id}.py", "w") as f:
                f.write(template.render(info))
with DAG(
    dag_id="googleTrend_api",
    start_date=datetime(2023, 8, 25),
    schedule=None,
    catchup=False
) as dag:
    generate_dags()