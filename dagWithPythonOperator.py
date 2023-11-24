from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

def t1():
    print ('task1 completed successfully')

def t2():
    print ('task2 completed successfully')

def t3():
    print ('task3 completed successfully')

with DAG (
    'second_dag_with_PythonOperator',
    start_date=datetime(2023,1,1),
    schedule = '@once',
    description='my second DAG with PythonOperator and python callable example',
    catchup=False
    ) as dag:

    t1 = PythonOperator (
        task_id = 'task1',
        python_callable = t1
    )

    t2 = PythonOperator (
        task_id = 'task2',
        python_callable = t2
    )

    t3 = PythonOperator (
        task_id = 'task3',
        python_callable = t3
    )
    
t1 >> [t2 ,t3]