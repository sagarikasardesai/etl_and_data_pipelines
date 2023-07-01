# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'xxx',
    'start_date': days_ago(0),
    'email': ['xxx@xxxx.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the task named extract_transform_load to call the shell script
extract_transform_load = BashOperator(
    task_id='extract_transform_load',
    bash_command='/home/project/airflow/dags/Extract_Transform_data.sh',
    dag=dag,
)

#task pipeline
extract_transform_load