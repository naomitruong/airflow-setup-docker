from airflow.decorators import dag,task
from datetime import datetime, timedelta
default_args={
 'owner':'naomitruong',
 'retries':5,
 'retry_delay':timedelta(minutes=2)
}
@dag(dag_id='dag_with_taskflow_api_v1',
     default_args=default_args,
     start_date=datetime(2025,10,29,7),
     schedule='@daily')

def hello_world_etl():


    @task()
    def get_name():
            return "Naomi Truong"
    @task()
    def get_age():
            return 22
    @task()
    def greet(name,age):
           print(f"Hello world!!! My name is {name} and I am {age} years old.")

    name=get_name()
    age=get_age()
    greet(name=name,age=age)

greet_dag=hello_world_etl()


    