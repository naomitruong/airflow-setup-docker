from airflow.decorators import dag,task
from datetime import datetime, timedelta
default_args={
 'owner':'naomitruong',
 'retries':5,
 'retry_delay':timedelta(minutes=2)
}
@dag(dag_id='dag_with_taskflow_api_v2',
     default_args=default_args,
     start_date=datetime(2025,10,29,7),
     schedule='@daily')

def hello_world_etl():


    @task(multiple_outputs=True)
    def get_name():
            return {'first_name':'Naomi', 'last_name':'Truong'}
    @task()
    def get_age():
            return 22
    @task()
    def greet(firstname, lastname,age):
           print(f"Hello world!!! My name is {firstname} {lastname} and I am {age} years old.")

    name_dict=get_name()
    age=get_age()
    greet(firstname=name_dict['first_name'],lastname=name_dict['last_name'],age=age)

greet_dag=hello_world_etl()


    