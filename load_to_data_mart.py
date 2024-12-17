from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Save Data Mart to Another Database") \
    .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
    .getOrCreate()

# URL dan properti koneksi PostgreSQL untuk database asal
source_jdbc_url = "jdbc:postgresql://127.0.0.1:5432/pentaho_db"
source_properties = {
    "user": "postgres",
    "password": "fikadiansari",
    "driver": "org.postgresql.Driver"
}

# URL dan properti koneksi PostgreSQL untuk database tujuan
destination_jdbc_url = "jdbc:postgresql://127.0.0.1:5432/data_mart"  
destination_properties = {
    "user": "postgres",
    "password": "fikadiansari",
    "driver": "org.postgresql.Driver"
}

try:
    # Baca data dari database asal
    data_mart_df = spark.read.jdbc(url=source_jdbc_url, table="data_mart", properties=source_properties)
    print("Data berhasil dimuat dari database asal (pentaho_db).")

    # Tampilkan data untuk memastikan isinya
    data_mart_df.show()

    # Simpan data ke database tujuan
    data_mart_df.write.jdbc(url=destination_jdbc_url, table="data_mart", mode="overwrite", properties=destination_properties)
    print("Data mart berhasil disimpan ke database tujuan (analytics_db) dalam tabel 'data_mart'.")

except Exception as e:
    print(f"Terjadi kesalahan: {e}")

finally:
    # Menutup Spark Session
    spark.stop()
    print("Spark Session ditutup.")