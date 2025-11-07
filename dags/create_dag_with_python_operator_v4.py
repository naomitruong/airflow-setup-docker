from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'naomitruong',
    'retries':5,
    'retry_delay':timedelta(minutes=2)

}
def greet(age, ti):
    name=ti.xcom_pull(task_ids='get_name' )
    print(f"Hello world!!! My name is {name} and I am {age} years old.")

def get_name():
    return "Jerry"


with DAG(
default_args=default_args,
dag_id='create_dag_with_python_operator_v4',
description='Our first DAG using Python Operator',
start_date=datetime(2025,10,29,7),
schedule='@daily'

) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age':22}
    )
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name,

    )
    task2 >> task1