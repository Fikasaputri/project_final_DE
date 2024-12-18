from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2

# Default args
default_args = {
    'owner': 'Fika',
    'depends_on_past': False,
    'email': ["fikasaputri818@gmail.com"],
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fungsi untuk menyimpan log ke PostgreSQL
def save_log_to_postgres(status, **kwargs):
    try:
        # Koneksi ke PostgreSQL
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="pentaho_db",
            user="postgres",
            password="fikadiansari"
        )
        cursor = conn.cursor()
        
        # Query untuk menyimpan log (tanpa menyuplai id_result, biarkan auto-increment)
        insert_query = """
        INSERT INTO etl_logs (datetime, status)
        VALUES (%s, %s)
        """
        current_time = datetime.now()
        cursor.execute(insert_query, (current_time, status))
        conn.commit()
        print(f"Log berhasil disimpan dengan status: {status}")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan log: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

# Fungsi untuk menjalankan Spark job
def run_spark_job(**kwargs):
    import subprocess

    try:
        # Perintah untuk menjalankan Spark job
        command = [
            "/home/hadoop/spark/bin/spark-submit",
            "--jars", "/home/hadoop/postgresql-42.2.26.jar",
            "/home/hadoop/airflow/dags/pyspark_processing.py"
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)  # Output dari Spark job
        save_log_to_postgres("SUCCESS")  # Simpan log jika sukses
    except subprocess.CalledProcessError as e:
        print(f"Spark job gagal dengan error: {e.stderr}")
        save_log_to_postgres("FAILED")  # Simpan log jika gagal
        raise

# Definisi DAG
with DAG(
    'etl_spark_with_logging',
    default_args=default_args,
    description='DAG untuk menjalankan Spark job dengan logging ke PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 15),
    catchup=False,
) as dag:

    # Task 1: Cheking koneksi PostgreSQL
    def check_postgres_connection():
        try:
            conn = psycopg2.connect(
                host="127.0.0.1",
                database="pentaho_db",
                user="postgres",
                password="fikadiansari"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            print("Koneksi PostgreSQL berhasil!")
        except Exception as e:
            print(f"Koneksi PostgreSQL gagal: {e}")
            raise
        finally:
            if conn:
                cursor.close()
                conn.close()

    check_postgres = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection
    )

    # Task 2: Menjalankan Spark job
    run_spark = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
        provide_context=True,
    )

    # Task 3: Menyimpan log ke PostgreSQL
    def finalize_log(status, **kwargs):
        save_log_to_postgres(status)

    finalize_success_log = PythonOperator(
        task_id='finalize_success_log',
        python_callable=finalize_log,
        op_args=["SUCCESS"],
        provide_context=True,
    )

    finalize_failed_log = PythonOperator(
        task_id='finalize_failed_log',
        python_callable=finalize_log,
        op_args=["FAILED"],
        provide_context=True,
        trigger_rule="one_failed",  
    )

    # Alur eksekusi task
    check_postgres >> run_spark >> [finalize_success_log, finalize_failed_log]