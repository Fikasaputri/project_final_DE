import json
from datetime import datetime, timedelta
import random

# Fungsi untuk menghasilkan ID pasien
def generate_pasien_id(urutan):
    return f"P{str(urutan).zfill(3)}"

# Fungsi untuk menghasilkan ID pendaftaran
def generate_pendaftaran_id(tanggal, urutan):
    return f"REG{tanggal.strftime('%Y%m%d')}{str(urutan).zfill(3)}"

# Daftar poli
poli_list = [
    "Poli Umum", 
    "Poli Anak", 
    "Poli Kandungan", 
    "Poli Gigi", 
    "Poli Mata", 
    "Poli Jantung",
    "Poli Saraf",
    "Poli Kulit"
]

# Metode pembayaran
metode_pembayaran = [
    "BPJS", 
    "Tunai", 
    "Asuransi Swasta", 
    "Transfer Bank"
]


# Generate data pendaftaran
daftar_pendaftaran = []
start_date = datetime(2024, 1, 1)  # Mulai dari 1 Januari 2024
end_date = datetime(2024, 6, 30)  # Hingga 30 Juni 2024
total_days = (end_date - start_date).days  # Total jumlah hari dari Jan-Jun 2024

# Generate data dengan tanggal berurutan dan jumlah pendaftaran per tanggal antara 10-20
pasien_counter = 1  # Untuk menghitung ID pasien yang terurut
for tanggal in (start_date + timedelta(days=i) for i in range(total_days + 1)):  # Loop dari 1 Januari 2024 hingga 30 Juni 2024
    num_visits = random.randint(10, 20)  # Jumlah kunjungan antara 10 dan 20 per tanggal
    
    for _ in range(num_visits):
        if pasien_counter > 1200:  # Pastikan jumlah pasien tidak melebihi 1200
            break
        
        pendaftaran = {
            "Pendaftaran ID": generate_pendaftaran_id(tanggal, pasien_counter),
            "Pasien ID": generate_pasien_id(pasien_counter),
            "Poli Tujuan": random.choice(poli_list),
            "Metode Pembayaran": random.choice(metode_pembayaran),
            "Tanggal Pendaftaran": tanggal.strftime("%Y-%m-%d ")
        }
        
        daftar_pendaftaran.append(pendaftaran)
        pasien_counter += 1  # Increment ID pasien

# Menyimpan data ke file JSON
with open('data_pendaftaran_multiple_visits.json', 'w', encoding='utf-8') as file:
    json.dump(daftar_pendaftaran, file, indent=4, ensure_ascii=False)

# Menampilkan total pendaftaran
print(f"Total pendaftaran: {len(daftar_pendaftaran)}")