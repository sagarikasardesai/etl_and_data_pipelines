from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'ssardesai',
    'start_date' : days_ago(0),
    'email' : ['xxxx@xxxx.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

dag=DAG(
    'ETL-Server-Access-Log-Processing',
    default_args = default_args,
    description = 'ETL Server Access Log Processing',
    schedule_interval = timedelta(days=1),
)

download = BashOperator(
    task_id = 'download',
    bash_command = 'wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag = dag,
)

extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -f1,4 -d"#" web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag = dag,
)

transform = BashOperator(
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalised.txt',
    dag = dag,
)

load = BashOperator(
    task_id = 'load',
    bash_command = 'zip log.zip capitalised.txt',
    dag=dag,
)

download >> extract >> transform >> load