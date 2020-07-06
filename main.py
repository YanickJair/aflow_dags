from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from vdf.src.csv_file_writer import write_to_csv
from vdf.src.load_dag_configs import load_dag_configs
from vdf.src.query_data_from_table import query_data
from vdf.src.sftp_sender import send_cvs_file_via_sftp
from vdf.src.clean_xcom_values import clean_xcom_values

default_args = {
    "owner": "YA",
    "start_date": datetime(2020, 1, 1),
    "email": "your@email.com",
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="sftp_dag",
        schedule_interval=timedelta(minutes=4),
        default_args=default_args,
        on_success_callback=clean_xcom_values
    ) as dag:

    start_task = DummyOperator(task_id='start_task')

    task_load_dagrun_configs = PythonOperator(
        task_id="load_dagrun_configs",
        python_callable=load_dag_configs,
        provide_context=True,
        dag=dag
    )

    # * Task to query all users from DB
    task_query_user_data = PythonOperator(
        task_id="query_data",
        python_callable=query_data,
        provide_context=True,
        dag=dag
    )

    # * Task to write the queried data to a CSV file
    task_write_user_to_csv = PythonOperator(
        task_id="write_data_to_csv",
        python_callable=write_to_csv,
        provide_context=True,
        dag=dag
    )

    task_send_csv_file = PythonOperator(
        task_id="send_csv_file",
        python_callable=send_cvs_file_via_sftp,
        provide_context=True,
        dag=dag
    )

    end_task = DummyOperator(task_id='end_task')

    # * Task orders
    start_task >> task_load_dagrun_configs >> task_query_user_data >> task_write_user_to_csv >> task_send_csv_file >> end_task
