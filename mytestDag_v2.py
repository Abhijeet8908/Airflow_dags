from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
from google.cloud import storage
import sys
from airflow.decorators import dag, task


@task(task_id = 'list_file')
def list_blobs_with_prefix(bucket_nm,prefix,delimiter):
    """Lists all the blobs in the bucket that begins with prefix"""
 
    storage_client = storage.Client()
    bucket_nm = storage_client.get_bucket(bucket_nm)
 
    blobs = storage_client.list_blobs(bucket_nm,prefix=prefix,delimiter=delimiter)
    blob_list=['FileName']
    for blob in blobs:
        # blob_list.append(blob.name)
        print(blob.name)

    if delimiter:
        print("Prefixes:")
        for prefix in blobs.prefixes:
            blob_list.append(prefix.split('/')[-1])
            print(prefix.split('/')[-1])  
 
    storage_client = storage.Client()
    blob = bucket_nm.blob("data.csv")
 
    with blob.open("w") as f:
        f.write(("\n".join(map(str,blob_list))))
 
list_file_task = list_blobs_with_prefix("my_test_bucket_876", "src/", ".txt")

with DAG (
    'my_test_copy_dag_v2',
    start_date=datetime(2023,1,1),
    schedule = '@once',
    catchup=False
    ) as dag:

    task1 = DummyOperator(
        task_id = 'Start',
    )

    task2 = DummyOperator(
        task_id = 'End',
    )

    data = pd.read_csv(f'gs://my_test_bucket_876/data.csv')
    
    for index, row in data.iterrows():
        fileName = row['FileName']
        year = fileName[-12:-8]
        month = fileName[-8:-6]
        date = fileName[-6:-4]
        task_ID = f'copying_file_{fileName}_to_{year}_{month}_{date}'
        # print("Here I am: " + task_ID)

        copy_task = BashOperator(
            task_id = task_ID,
            bash_command = f'gsutil cp gs://my_test_bucket_876/src/{fileName} gs://my_test_bucket_876/dest/{year}/{month}/{date}/'
        )

        task1 >> list_file_task >> copy_task >>task2