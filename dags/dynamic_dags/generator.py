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
    template = env.get_template('github_api.jinja2')

    for f in os.listdir(f"{file_dir}/config"):
        if f.endswith(".yml"):
            with open(f"{file_dir}/config/{f}", "r") as cf:
                config = yaml.safe_load(cf)
                with open(f"{dag_dir}/github_{config['dag_id']}.py", "w") as f:
                    f.write(template.render(config))

with DAG(
    dag_id="github_api",
    start_date=datetime(2023, 6, 15),
    schedule='@once',
    catchup=False
) as dag:
    generate_dags()