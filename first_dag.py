from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator

with DAG (
    'my_first_dag',
    start_date=datetime(2023,1,1),
    schedule = '@once',
    catchup=False
    ) as dag:

    t1 = BashOperator (
        task_id = 'task1',
        bash_command='echo this_executes_t1'
    )

    t2 = BashOperator (
        task_id = 'task2',
        bash_command='echo this_executes_t2'
    )

    t3 = BashOperator (
        task_id = 'task3',
        bash_command='echo this_executes_t3'
    )

t1 >> t2 >>t3