from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args={
    'owner':'naomitruong',
    'retries':5,
    'retry_delay':timedelta(minutes=2)

}

with DAG(
    dag_id='dag_with_postgres_operator_v1',
    default_args=default_args,
    start_date=datetime(2025,11,1,7),
    schedule='0 0 * * *'
) as dag:
    task1=SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_localhost',
        sql="""
        create table if not exists dag_runs(
        dt date,
        dag_id character varying(250),
        primary key (dt,dag_id)
        )  
"""
    )

    task2=SQLExecuteQueryOperator(
    task_id='insert_into_table',
    conn_id='postgres_localhost',
    sql="""
            INSERT INTO dag_runs(dt, dag_id)
            VALUES ('{{ params.ds }}', '{{ params.dag_id }}');
        """,
        params={
            "ds": "{{ ds }}",
            "dag_id": "{{ dag.dag_id }}"
        }
        )
    
    task1 >> task2