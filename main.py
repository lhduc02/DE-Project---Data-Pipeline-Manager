from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import gzip
import os
import pandas as pd
import json
from pymongo import MongoClient
import shutil


# Định nghĩa thông số cơ bản cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_mongodb_to_bigquery_pipeline',
    default_args=default_args,
    description='A simple pipeline to move data from MongoDB to GCS and BigQuery',
    schedule_interval='0 7 * * *',  # Chạy hàng ngày lúc 7:00 AM giờ Việt Nam
)

# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['local']
collection = db['data_test']

# Tạo file CSV từ MongoDB
def download_and_convert_data(**kwargs):
    file_path = "C:\\Users\\ADMIN\\Downloads\\Storage\\data_test.ndjson"
    with open(file_path, 'w') as file:
        cursor = collection.find({})
        for document in cursor:
            file.write(json.dumps(document) + '\n')
    
    csv_file = file_path.replace('.ndjson', '.csv')
    with open(file_path, 'r') as f:
        data = [json.loads(line) for line in f]
    
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)
    return csv_file

# Nén dữ liệu với gzip
def compress_file(csv_file):
    gz_file = csv_file + '.gz'
    with open(csv_file, 'rb') as f_in:
        with gzip.open(gz_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return gz_file

# Upload dữ liệu lên Google Cloud Storage
def upload_to_gcs(bucket_name, gz_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.basename(gz_file))
    blob.upload_from_filename(gz_file)

# Import dữ liệu vào BigQuery
def load_data_to_bigquery(gz_file, dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open(gz_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Đợi cho đến khi job hoàn thành

# Task 1: Tải và chuyển đổi dữ liệu từ MongoDB
t1 = PythonOperator(
    task_id='download_and_convert_data',
    python_callable=download_and_convert_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Nén dữ liệu
t2 = PythonOperator(
    task_id='compress_file',
    python_callable=compress_file,
    provide_context=True,
    dag=dag,
)

# Task 3: Upload lên GCS
t3 = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={'bucket_name': 'gcs-bucket'},
    provide_context=True,
    dag=dag,
)

# Task 4: Import vào BigQuery
t4 = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    op_kwargs={'dataset_id': 'datapipelineproject-435505', 'table_id': 'data_users'},
    provide_context=True,
    dag=dag,
)

# Thiết lập thứ tự task
t1 >> t2 >> t3 >> t4
