from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Fika',
    'depends_on_past': False,
    'email': ["fikasaputri818@gmail.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_spark',
    default_args=default_args,
    description='DAG untuk menjalankan Spark job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 15),
    catchup=False,
)

spark_job = BashOperator(
    task_id='run_spark_transformasi',
    bash_command="""
    /home/hadoop/spark/bin/spark-submit \
    --jars /home/hadoop/postgresql-42.2.26.jar \
    /home/hadoop/airflow/dags/pyspark_processing.py
    """,
    dag=dag,
)

spark_job

