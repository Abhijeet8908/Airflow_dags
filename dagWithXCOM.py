from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.utils.email import send_email

def t1():
    print ('task1 completed successfully')
    return 1

def t2() -> str:
    # print ('task2 completed successfully')
    return str(datetime.now())

def t3(ti):
    capturedValue = ti.xcom_pull(task_ids=['task2'])
    if not capturedValue:
        raise ValueError('No value currently stored in XComs.')
    print (f'Captured value : {capturedValue}')
    print ('task3 completed successfully')

with DAG (
    'dag_with_XCOM',
    start_date=datetime(2023,1,1),
    schedule = '@once',
    description='my third DAG with xcom',    
    catchup=False
    ) as dag:

    task1 = PythonOperator (
        task_id = 'task1',
        python_callable = t1
    )

    task2 = PythonOperator (
        task_id = 'task2',
        python_callable = t2
        # do_xcom_push = True
    )

    task3 = PythonOperator (
        task_id = 'task3',
        python_callable = t3
    )
    
task1 >> task2 >> task3