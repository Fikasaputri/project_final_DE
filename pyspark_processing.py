from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("PostgreSQL ETL Job with Masking and Column Ordering") \
    .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
    .getOrCreate()

# URL dan properti koneksi PostgreSQL
jdbc_url = "jdbc:postgresql://127.0.0.1:5432/pentaho_db"
properties = {
    "user": "postgres",
    "password": "fikadiansari",
    "driver": "org.postgresql.Driver"
}

try:
    # Baca data dari PostgreSQL
    pendaftaran_df = spark.read.jdbc(url=jdbc_url, table="data_pendaftaran", properties=properties)
    gejala_to_diagnosis_df = spark.read.jdbc(url=jdbc_url, table="gejala_to_diagnosis", properties=properties)
    diagnosis_to_obat_df = spark.read.jdbc(url=jdbc_url, table="diagnosis_to_obat", properties=properties)
    pasien_df = spark.read.jdbc(url=jdbc_url, table="data_pasien", properties=properties)
    
    print("Data berhasil dimuat dari PostgreSQL.")

    # Masking untuk nomor telepon dalam format +62**********98
    pasien_df = pasien_df.withColumn(
        "nomor_telepon_masked",
        F.concat(F.lit("+62"), F.lit("**********"), F.substring("nomor_telepon", -2, 2))
    )

    # Masking untuk email (contoh: fik*********@gmail.com)
    pasien_df = pasien_df.withColumn(
        "email_masked",
        F.concat(
            F.substring("email", 1, 3),  
            F.lit("*********"),         
            F.lit("@"),                
            F.split("email", "@").getItem(1)  
        )
    )

    # Join data pendaftaran dengan data pasien
    pendaftaran_pasien_df = pendaftaran_df.join(pasien_df, pendaftaran_df.id_pasien == pasien_df.id_pasien, "left") \
        .select(
            pendaftaran_df["*"], 
            pasien_df["nama_lengkap"].alias("nama_pasien"),  
            pasien_df["umur"].alias("usia"),               
            pasien_df["jenis_kelamin"],
            pasien_df["pekerjaan"],                        
            pasien_df["email_masked"].alias("email"),      
            pasien_df["nomor_telepon_masked"].alias("nomor_telepon"),  
            pasien_df["kecamatan"]                        
        )
    
    print("Join data pendaftaran dengan data pasien berhasil.")

    # Join data pendaftaran_pasien dengan gejala_to_diagnosis
    joined_df = pendaftaran_pasien_df.join(gejala_to_diagnosis_df, pendaftaran_pasien_df.gejala == gejala_to_diagnosis_df.gejala, "left") \
        .select(pendaftaran_pasien_df["*"], gejala_to_diagnosis_df["diagnosis"])
    
    print("Join data pendaftaran_pasien dengan gejala_to_diagnosis berhasil.")

    # Join data dengan diagnosis_to_obat
    final_df = joined_df.join(diagnosis_to_obat_df, joined_df.diagnosis == diagnosis_to_obat_df.diagnosis, "left") \
        .select(joined_df["*"], diagnosis_to_obat_df["obat"])

    print("Join data diagnosis dengan diagnosis_to_obat berhasil.")

    # Mengatur urutan kolom
    ordered_df = final_df.select(
        "id_pendaftaran",
        "tanggal_pendaftaran",
        "id_pasien",
        "nama_pasien",
        "jenis_kelamin",
        "usia",
        "kecamatan",
        "pekerjaan",
        "nomor_telepon",
        "email",
        "poli_tujuan",
        "metode_pembayaran",
        "gejala",
        "diagnosis",
        "obat"
    )

    # Menampilkan hasil dengan urutan kolom yang benar
    ordered_df.show()

    # Menyimpan hasil ke PostgreSQL
    ordered_df.write.jdbc(url=jdbc_url, table="data_mart", mode="overwrite", properties=properties)
    
    print("Data berhasil disimpan ke PostgreSQL dalam tabel 'data_mart'.")
    
except Exception as e:
    print(f"Terjadi kesalahan: {e}")
    
finally:
    # Menutup Spark Session
    spark.stop()
    print("Spark Session ditutup.")