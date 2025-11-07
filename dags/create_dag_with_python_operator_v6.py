from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'naomitruong',
    'retries':5,
    'retry_delay':timedelta(minutes=2)

}
def greet(ti):
    first_name=ti.xcom_pull(key='first_name',task_ids='get_name')
    last_name=ti.xcom_pull(key='last_name',task_ids='get_name')
    age=ti.xcom_pull(key='age',task_ids='get_age')
    name=ti.xcom_pull(task_ids='get_name' )
    print(f"Hello world!!! My name is {first_name} {last_name} and I am {age} years old.")

def get_name(ti):
    ti.xcom_push(key='first_name',value='Naomi')
    ti.xcom_push(key='last_name',value='Truong')
    return "Jerry"
def get_age(ti):
    ti.xcom_push(key='age',value=22)

with DAG(
default_args=default_args,
dag_id='create_dag_with_python_operator_v6',
description='Our first DAG using Python Operator',
start_date=datetime(2025,10,29,7),
schedule='@daily'

) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet
    )
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name,

    )
    task3 = PythonOperator(task_id='get_age', python_callable=get_age)
    [task3,task2] >> task1