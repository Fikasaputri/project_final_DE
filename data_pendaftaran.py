import json
from datetime import datetime, timedelta
import random

# Fungsi untuk menghasilkan ID pasien
def generate_pasien_id(urutan):
    return f"P{str(urutan).zfill(3)}"

# Fungsi untuk menghasilkan ID pendaftaran
def generate_pendaftaran_id(tanggal, urutan):
    return f"REG{tanggal.strftime('%Y%m%d')}{str(urutan).zfill(3)}"

# Daftar poli dan gejala terkait
poli_gejala_map = {
    "Poli Umum": [
        "Demam", "Nyeri tubuh atau sendi", "Batuk dan pilek", "Sesak napas",
        "Lemas atau kelelahan", "Mual atau muntah", "Kehilangan nafsu makan",
        "Sakit tenggorokan", "Diare atau sembelit", "Penurunan berat badan tanpa alasan jelas"
    ],
    "Poli Anak": [
        "Demam tinggi", "Batuk atau pilek yang persisten", "Muntah atau diare", 
        "Ruam kulit", "Penurunan nafsu makan", "Sakit perut", "Napas cepat atau kesulitan bernapas", 
        "Gangguan tidur atau iritabilitas", "Cemas atau tidak nyaman", 
        "Perkembangan fisik atau motorik yang terlambat"
    ],
    "Poli Kandungan": [
        "Nyeri perut bawah atau panggul", "Perubahan siklus menstruasi", 
        "Pendarahan abnormal dari vagina", "Rasa sakit atau ketidaknyamanan saat berhubungan intim", 
        "Perubahan dalam kadar hormon", "Keputihan tidak normal", 
        "Mual dan muntah (terutama pada kehamilan)", "Kram perut atau nyeri punggung bagian bawah", 
        "Pembengkakan atau rasa berat pada perut"
    ],
    "Poli Gigi": [
        "Nyeri gigi atau gusi", "Pendarahan gusi", "Gigi goyang atau retak", 
        "Pembengkakan atau infeksi pada gusi", "Sensitivitas pada gigi", 
        "Nafas tak sedap", "Sakit kepala terkait masalah gigi", 
        "Kesulitan mengunyah atau membuka mulut", "Perubahan warna pada gigi"
    ],
    "Poli Mata": [
        "Penglihatan kabur atau berkurang", "Mata merah atau iritasi", 
        "Nyeri mata", "Sensasi ada benda asing di mata", 
        "Keluar cairan dari mata", "Fotofobia", "Gatal atau rasa terbakar pada mata", 
        "Penglihatan ganda", "Kesulitan melihat di malam hari", "Mata berair berlebihan"
    ],
    "Poli Jantung": [
        "Nyeri dada atau ketidaknyamanan", "Sesak napas", "Detak jantung tidak teratur atau berdebar", 
        "Pusing atau pingsan", "Pembengkakan di kaki atau pergelangan kaki", 
        "Kelelahan ekstrem", "Mual atau muntah", 
        "Nyeri di lengan, punggung, rahang, atau leher", 
        "Peningkatan detak jantung saat istirahat", "Batuk berdahak atau berdarah"
    ],
    "Poli Saraf": [
        "Sakit kepala berat atau migrain", "Kesulitan bergerak atau koordinasi tubuh", 
        "Pusing atau vertigo", "Kehilangan sensasi atau mati rasa di bagian tubuh tertentu", 
        "Gangguan berbicara atau memahami pembicaraan", "Kesulitan memori atau kebingungan", 
        "Kehilangan keseimbangan atau berjalan pincang", "Kejang atau tremor", 
        "Mati rasa atau kelemahan otot"
    ],
    "Poli Kulit": [
        "Ruam atau perubahan warna kulit", "Gatal atau terbakar", "Jerawat atau komedo", 
        "Kulit kering atau bersisik", "Benjolan atau tumor kulit", 
        "Kerontokan rambut", "Luka atau goresan yang sulit sembuh", 
        "Kulit sensitif atau iritasi", "Pembengkakan atau peradangan", 
        "Kutil atau tahi lalat yang berubah bentuk atau warna"
    ]
}

# Metode pembayaran
metode_pembayaran = [
    "BPJS", "Tunai", "Asuransi Swasta", "Transfer Bank"
]

# Generate data pendaftaran
daftar_pendaftaran = []
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 6, 30)
total_days = (end_date - start_date).days

pasien_counter = 1
for tanggal in (start_date + timedelta(days=i) for i in range(total_days + 1)):
    num_visits = random.randint(10, 20)  
    for _ in range(num_visits):
        if pasien_counter > 1200:
            break
        poli_tujuan = random.choice(list(poli_gejala_map.keys()))
        gejala = random.choice(poli_gejala_map[poli_tujuan])
        pendaftaran = {
            "Pendaftaran ID": generate_pendaftaran_id(tanggal, pasien_counter),
            "Pasien ID": generate_pasien_id(pasien_counter),
            "Poli Tujuan": poli_tujuan,
            "Metode Pembayaran": random.choice(metode_pembayaran),
            "Tanggal Pendaftaran": tanggal.strftime("%Y-%m-%d"),
            "Gejala": gejala
        }
        daftar_pendaftaran.append(pendaftaran)
        pasien_counter += 1

# Menyimpan data ke file JSON
with open('data_pendaftaran.json', 'w', encoding='utf-8') as file:
    json.dump(daftar_pendaftaran, file, indent=4, ensure_ascii=False)

# Menampilkan total pendaftaran
print(f"Total pendaftaran: {len(daftar_pendaftaran)}")