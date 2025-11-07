from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args={
    'owner':'naomitruong',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_cron_expression_v1',
    default_args=default_args,
    start_date=datetime(2025,11,1,7),
    schedule='0 3 * * *' 
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo This is an simple bash command for task1'
    )