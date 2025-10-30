from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator

default_args={
'owner':'naomitruong',
'retries':5,
'retry_delay':timedelta(minutes=2)
}

with DAG(
dag_id='our_first_dag_v3',
default_args=default_args,
description='Our First DAG',
start_date=datetime(2025,10,29,7),
schedule='@daily'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo Hello world, this is the first task!'"
    )
    task2=BashOperator(
        task_id='second_task',
        bash_command="echo Hey, I am task2 and will be running after the task1!"
    )
    task3=BashOperator(
        task_id='third_task',
        bash_command="echo Hey, I am task3 and will be running after the task1!"
    )

    task1.set_downstream([task2,task3])