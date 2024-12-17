from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from airflow.models import Variable


default_args = {
    'owner': 'Cecep',
    'depends_on_past': False,
    'email': ['adececep35@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Mapping gejala ke diagnosis
gejala_to_diagnosis = {
    "Demam": "Infeksi Saluran Pernapasan Atas (ISPA)",
    "Nyeri tubuh atau sendi": "Infeksi Saluran Pernapasan Atas (ISPA)",
    "Batuk dan pilek": "Infeksi Saluran Pernapasan Atas (ISPA)",
    "Sesak napas": "Penyakit Jantung Koroner",
    "Lemas atau kelelahan": "Demam Tifoid (Tipes)",
    "Mual atau muntah": "Gastroenteritis atau Demam Tifoid (Tipes)",
    "Kehilangan nafsu makan": "Demam Tifoid (Tipes)",
    "Sakit tenggorokan": "Infeksi Saluran Pernapasan Atas (ISPA)",
    "Diare atau sembelit": "Diare Akut",
    "Penurunan berat badan tanpa alasan jelas": "Penyakit Kronis atau Gangguan Pencernaan",

    "Demam tinggi": "Infeksi Saluran Pernapasan Akut (ISPA)",
    "Batuk atau pilek yang persisten": "Infeksi Saluran Pernapasan Akut (ISPA)",
    "Muntah atau diare": "Diare Akut",
    "Ruam kulit": "Dermatitis Kontak",
    "Penurunan nafsu makan": "Diare Akut",
    "Sakit perut": "Diare Akut",
    "Napas cepat atau kesulitan bernapas": "Infeksi Saluran Pernapasan Akut (ISPA)",
    "Gangguan tidur atau iritabilitas": "Gangguan Tidur atau Infeksi Saluran Pernapasan Akut (ISPA)",
    "Cemas atau tidak nyaman": "Gangguan Kecemasan atau Infeksi Saluran Pernapasan Akut (ISPA)",
    "Perkembangan fisik atau motorik yang terlambat": "Gangguan Perkembangan Anak",

    "Nyeri perut bawah atau panggul": "Endometriosis",
    "Perubahan siklus menstruasi": "Gangguan Hormonal atau Endometriosis",
    "Pendarahan abnormal dari vagina": "Endometriosis",
    "Rasa sakit atau ketidaknyamanan saat berhubungan intim": "Endometriosis",
    "Perubahan dalam kadar hormon": "Gangguan Hormonal",
    "Keputihan tidak normal": "Infeksi Saluran Reproduksi (Vaginitis)",
    "Mual dan muntah (terutama pada kehamilan)": "Kehamilan atau Mual pada Kehamilan",
    "Kram perut atau nyeri punggung bagian bawah": "Endometriosis",
    "Pembengkakan atau rasa berat pada perut": "Gangguan Pencernaan atau Kehamilan",

    "Nyeri gigi atau gusi": "Periodontitis (Infeksi Gusi)",
    "Pendarahan gusi": "Periodontitis (Infeksi Gusi)",
    "Gigi goyang atau retak": "Abses Gigi",
    "Pembengkakan atau infeksi pada gusi": "Periodontitis (Infeksi Gusi)",
    "Sensitivitas pada gigi": "Sensitivitas Gigi",
    "Nafas tak sedap": "Periodontitis (Infeksi Gusi)",
    "Sakit kepala terkait masalah gigi": "Masalah Gigi atau Periodontitis",
    "Kesulitan mengunyah atau membuka mulut": "Abses Gigi",
    "Perubahan warna pada gigi": "Masalah Gigi atau Periodontitis",

    "Penglihatan kabur atau berkurang": "Gangguan Penglihatan atau Katarak",
    "Mata merah atau iritasi": "Konjungtivitis (Mata Merah)",
    "Nyeri mata": "Keratitis (Infeksi Kornea)",
    "Sensasi ada benda asing di mata": "Konjungtivitis atau Iritasi Mata",
    "Keluar cairan dari mata": "Konjungtivitis (Mata Merah)",
    "Fotofobia": "Keratitis (Infeksi Kornea)",
    "Gatal atau rasa terbakar pada mata": "Konjungtivitis (Mata Merah)",
    "Penglihatan ganda": "Gangguan Penglihatan atau Stroke Iskemik",
    "Kesulitan melihat di malam hari": "Retinopati atau Gangguan Penglihatan",
    "Mata berair berlebihan": "Konjungtivitis (Mata Merah)",

    "Nyeri dada atau ketidaknyamanan": "Penyakit Jantung Koroner",
    "Sesak napas": "Penyakit Jantung Koroner",
    "Detak jantung tidak teratur atau berdebar": "Penyakit Jantung Koroner atau Gangguan Irama Jantung",
    "Pusing atau pingsan": "Penyakit Jantung Koroner atau Hipotensi",
    "Pembengkakan di kaki atau pergelangan kaki": "Penyakit Jantung Koroner atau Gagal Jantung",
    "Kelelahan ekstrem": "Penyakit Jantung Koroner atau Gangguan Jantung",
    "Mual atau muntah": "Penyakit Jantung Koroner atau Gagal Jantung",
    "Nyeri di lengan, punggung, rahang, atau leher": "Penyakit Jantung Koroner",
    "Peningkatan detak jantung saat istirahat": "Gangguan Irama Jantung",
    "Batuk berdahak atau berdarah": "Penyakit Paru Obstruktif Kronis (PPOK)",

    "Sakit kepala berat atau migrain": "Migrain",
    "Kesulitan bergerak atau koordinasi tubuh": "Gangguan Neurologis atau Stroke Iskemik",
    "Pusing atau vertigo": "Gangguan Vestibular",
    "Kehilangan sensasi atau mati rasa di bagian tubuh tertentu": "Stroke Iskemik atau Gangguan Saraf Perifer",
    "Gangguan berbicara atau memahami pembicaraan": "Stroke Iskemik",
    "Kesulitan memori atau kebingungan": "Gangguan Memori atau Demensia",
    "Kehilangan keseimbangan atau berjalan pincang": "Gangguan Saraf atau Stroke Iskemik",
    "Kejang atau tremor": "Epilepsi atau Gangguan Saraf Lainnya",
    "Mati rasa atau kelemahan otot": "Stroke Iskemik atau Gangguan Neuromuskular",

    "Ruam atau perubahan warna kulit": "Dermatitis Kontak",
    "Gatal atau terbakar": "Dermatitis Kontak",
    "Jerawat atau komedo": "Akne Vulgaris",
    "Kulit kering atau bersisik": "Psoriasis atau Dermatitis",
    "Benjolan atau tumor kulit": "Kanker Kulit atau Lipoma",
    "Kerontokan rambut": "Alopecia Areata atau Efeks Samping Obat",
    "Luka atau goresan yang sulit sembuh": "Infeksi Kulit",
    "Kulit sensitif atau iritasi": "Dermatitis Kontak",
    "Pembengkakan atau peradangan": "Dermatitis atau Infeksi Kulit",
    "Kutil atau tahi lalat yang berubah bentuk atau warna": "Kanker Kulit (Melanoma)"
}


# Mapping diagnosis ke obat
diagnosis_to_obat = {
    # Penyakit Infeksi
    "Infeksi Saluran Pernapasan Akut (ISPA)": "Paracetamol, Dekongestan, Antibiotik (jika ada infeksi bakteri)",
    "Infeksi Saluran Pernapasan Atas (ISPA)": "Paracetamol, Antihistamin, Antibiotik (jika diperlukan)",
    "Infeksi Saluran Reproduksi (Vaginitis)": "Antifungal (ketoconazole), Antibiotik, Probiotik",
    "Infeksi Kulit": "Antibiotik topikal (mupirocin), Antibiotik oral (amoksisilin, cefadroxil), Antiseptik kulit (chlorhexidine)",
    "Dermatitis atau Infeksi Kulit": "Kortikosteroid topikal, Antihistamin, Antibiotik topikal jika diperlukan",
    "Gastroenteritis": "Oralit, Probiotik, Antibiotik jika diperlukan",
    "Demam Tifoid (Tipes)": "Kloramfenikol, Ceftriaxone, Paracetamol",
    "Konjungtivitis (Mata Merah)": "Tetes mata antibiotik, Tetes mata antiinflamasi, Antihistamin",
    "Konjungtivitis atau Iritasi Mata": "Tetes mata antibiotik, Tetes mata pelumas, Antihistamin",
    "Keratitis (Infeksi Kornea)": "Antibiotik mata, Antijamur mata",
    "Periodontitis (Infeksi Gusi)": "Metronidazol, Amoksisilin, Antiseptik mulut",
    "Masalah Gigi atau Periodontitis": "Amoksisilin, Metronidazol, Pereda nyeri, Antiseptik mulut",
    "Diare Akut": "Oralit, Loperamid, Probiotik",
    "Abses Gigi": "Amoksisilin, Metronidazol, Pereda nyeri",

    # Penyakit Kronis
    "Penyakit Jantung Koroner": "Aspirin, Nitrat, Beta-blocker",
    "Penyakit Jantung Koroner atau Gagal Jantung": "ACE inhibitor, Diuretik, Beta-blocker",
    "Penyakit Jantung Koroner atau Hipotensi": "Aspirin, ACE inhibitor, Terapi cairan",
    "Gangguan Irama Jantung": "Amiodaron, Beta-blocker, Obat antiaritmia lainnya",
    "Penyakit Paru Obstruktif Kronis (PPOK)": "Bronkodilator, Steroid inhalasi, Antibiotik",
    "Gangguan Hormonal": "Pil hormonal, Spironolakton, Metformin",
    "Gangguan Hormonal atau Endometriosis": "Pil hormonal, NSAID, GnRH agonist",
    "Gangguan Penglihatan atau Katarak": "Vitamin A, Operasi katarak, Kacamata koreksi",
    "Kanker Kulit (Melanoma)": "Kemoterapi, Imunoterapi, Operasi pengangkatan tumor",
    "Penyakit Jantung Koroner atau Gangguan Jantung": "Aspirin, Nitrat, Beta-blocker, ACE inhibitor, Statin, Diuretik",
    "Penyakit Jantung Koroner atau Gangguan Irama Jantung": "Aspirin, Beta-blocker, ACE inhibitor, Antiaritmia (Amiodaron, Flecainide), Statin, Diuretik",

    # Penyakit Neurologis
    "Migrain": "Paracetamol, Triptan, NSAID",
    "Epilepsi atau Gangguan Saraf Lainnya": "Karbamazepin, Valproat, Lamotrigin",
    "Stroke Iskemik": "Aspirin, Statin, Antikoagulan",
    "Stroke Iskemik atau Gangguan Neuromuskular": "Rehabilitasi neurologis, Aspirin, Statin",
    "Gangguan Neurologis atau Stroke Iskemik": "Rehabilitasi neurologis, Aspirin, Statin",
    "Stroke Iskemik atau Gangguan Saraf Perifer": "Aspirin, Statin, Rehabilitasi neurologis",
    "Gangguan Vestibular": "Betahistine, Dimenhydrinate, Rehabilitasi vestibular",
    "Gangguan Kecemasan atau Infeksi Saluran Pernapasan Akut (ISPA)": "Benzodiazepin, SSRI, Terapi psikologi",
    "Gangguan Penglihatan atau Stroke Iskemik": "Aspirin, Statin, Terapi antikoagulan, Vitamin A, Rehabilitasi visual",
    "Retinopati atau Gangguan Penglihatan": "Vitamin A, Anti-VEGF (ranibizumab, bevacizumab), Kortikosteroid intravitreal",
    "Gangguan Saraf atau Stroke Iskemik": "Aspirin, Statin, Terapi antikoagulan, Obat neuroprotektif, Rehabilitasi neurologis",

    # Penyakit Dermatologis
    "Dermatitis Kontak": "Antihistamin, Kortikosteroid topikal, Emolien",
    "Psoriasis atau Dermatitis": "Kortikosteroid topikal, Pelembap, Vitamin D analog",
    "Kanker Kulit atau Lipoma": "Operasi, Kemoterapi, Radioterapi",
    "Jerawat atau Komedo (Akne Vulgaris)": "Benzoyl peroxide, Isotretinoin, Salicylic acid",
    "Akne Vulgaris": "Benzoyl peroxide, Retinoid topikal, Antibiotik topikal atau oral",
    "Alopecia Areata atau Efeks Samping Obat": "Minoxidil, Kortikosteroid topikal, Suplemen biotin",

    # Penyakit Wanita
    "Endometriosis": "NSAID, Pil hormonal, GnRH agonist",
    "Kehamilan atau Mual pada Kehamilan": "Vitamin prenatal, Vitamin B6, Doxylamine",
    "Gangguan Pencernaan atau Kehamilan": "Antasida, Vitamin prenatal, Proton pump inhibitor (PPI)",

    # Gangguan Pencernaan
    "Gangguan Pencernaan": "Antasida, Proton pump inhibitor (PPI), H2 blocker",
    "Penyakit Kronis atau Gangguan Pencernaan": "Antasida, Proton pump inhibitor (PPI), Diet tinggi serat",

    # Gangguan Anak-Anak
    "Gangguan Perkembangan Anak": "Terapi fisik, Terapi okupasi, Vitamin dan suplemen",

    # Lainnya
    "Gangguan Tidur atau Infeksi Saluran Pernapasan Akut (ISPA)": "Antihistamin, Paracetamol, Dekongestan",
    "Gangguan Memori atau Demensia": "Donepezil, Memantine, Terapi memori",
    "Sensitivitas Gigi": "Pasta gigi desensitisasi, Fluoride gel, Perawatan gigi lanjutan"
}

# Fungsi untuk mengambil dan mentransformasi data
def fetch_transform_data():

    # Mendapatkan kunci enkripsi dari Airflow Variable
    encryption_key = Variable.get("ENCRYPTION_KEY")
    fernet = Fernet(encryption_key)

    # Koneksi ke database
    conn = BaseHook.get_connection('postgres_conn')
    try:
        db_conn = psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        cursor = db_conn.cursor()

        # Ambil data pendaftaran
        query_pendaftaran = """
            SELECT id_pendaftaran, poli_tujuan, gejala, id_pasien 
            FROM data_pendaftaran;
        """
        cursor.execute(query_pendaftaran)
        pendaftaran_data = cursor.fetchall()
        pendaftaran_df = pd.DataFrame(pendaftaran_data, columns=[
            'id_pendaftaran', 'poli_tujuan', 'gejala', 'id_pasien'
        ])

        # Mapping gejala ke diagnosis
        pendaftaran_df['diagnosis'] = pendaftaran_df['gejala'].map(gejala_to_diagnosis)

        # Tambahkan kolom Obat yang Disarankan
        pendaftaran_df['obat'] = pendaftaran_df['diagnosis'].map(diagnosis_to_obat)

        # Ambil data pasien
        query_pasien = """
            SELECT id_pasien, nama_lengkap, umur, jenis_kelamin, kecamatan, pekerjaan, email, nomor_telepon 
            FROM data_pasien;
        """
        cursor.execute(query_pasien)
        pasien_data = cursor.fetchall()
        pasien_df = pd.DataFrame(pasien_data, columns=[
            'id_pasien', 'nama_lengkap', 'umur', 'jenis_kelamin', 'kecamatan', 
            'pekerjaan', 'email', 'nomor_telepon'
        ])

        # Gabungkan dengan data pendaftaran untuk mendapatkan data mart
        data_mart = pendaftaran_df.merge(
            pasien_df, 
            on='id_pasien', 
            how='left'
        )

        # Menyusun kolom sesuai urutan yang diinginkan
        data_mart = data_mart[[
            'id_pendaftaran', 'id_pasien', 'nama_lengkap', 'umur', 'jenis_kelamin', 
            'kecamatan', 'pekerjaan', 'email', 'nomor_telepon', 'poli_tujuan', 
            'gejala', 'diagnosis', 'obat'
        ]]

          # Enkripsi nama_lengkap
        data_mart['nama_lengkap'] = data_mart['nama_lengkap'].apply(lambda x: fernet.encrypt(x.encode()).decode())

        # Masking email (hanya menampilkan karakter sebelum @)
        data_mart['email'] = data_mart['email'].apply(lambda x: f"{x.split('@')[0][:2]}@{x.split('@')[1]}")

        # Masking nomor_telepon (hanya menampilkan 4 digit terakhir)
        data_mart['nomor_telepon'] = data_mart['nomor_telepon'].apply(lambda x: x[:2] + "*" * (len(x) - 4) + x[-2:]
    )

        # Simpan hasil ke file CSV
        data_mart.to_csv('/home/hadoop/airflow/dags/data_mart.csv', index=False)

    except Exception as e:
        raise RuntimeError(f"Error during data fetch/transform: {e}")
    finally:
        if db_conn:
            cursor.close()
            db_conn.close()


# Definisikan DAG
with DAG(
    'transform_data_dag',
    default_args=default_args,
    description='Transformasi Data Pendaftaran dan Pasien',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 10),
    catchup=False,
) as dag:
    task_fetch_transform = PythonOperator(
        task_id='fetch_transform_data',
        python_callable=fetch_transform_data
    )

task_fetch_transform