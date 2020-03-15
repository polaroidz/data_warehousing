from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

airflow_args = {
    'owner': 'Airflow',
    'start_date': days_ago(2)
}

with DAG(
    dag_id="teste3",
    default_args=airflow_args,
    description="teste2",
    schedule_interval=None
) as dag:

    t1 = BashOperator(
        task_id='print_date1',
        bash_command='date',
        dag=dag
    )
    
    t2 = BashOperator(
        task_id='print_date2',
        bash_command='date',
        dag=dag
    )
    
    t1 >> t2
