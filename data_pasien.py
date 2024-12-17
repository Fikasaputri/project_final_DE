import json
import random
from faker import Faker

# Inisialisasi Faker untuk menghasilkan data palsu dalam bahasa Indonesia
fake = Faker('id_ID')

# Fungsi untuk menghasilkan ID pasien
def generate_pasien_id(urutan):
    return f"P{str(urutan).zfill(3)}"

# Daftar pekerjaan tambahan dalam bahasa Indonesia
profesi = [
    'Pengembang Perangkat Lunak', 'Guru', 'Dokter', 'Akuntan', 'Desainer', 
    'Arsitek', 'Marketing', 'Pengacara', 'Penulis', 'Fotografer', 
    'Insinyur', 'Peneliti', 'Konsultan', 'Pegawai Bank', 'Manajer'
]

kecamatan = [
    'Ciawigebang','Cibeureum','Cibingbin','Cidahu','Cigandamekar',
    'Cigugur','Cilebak','Cilimus','Cimahi','Ciniru','Cipicung','Ciwaru',
    'Darma','Garawangi','Hantara','Jalaksana','Japara','Kadugede','Kalimanggis',
    'Karangkancana','Kramatmulya','Kuningan','Lebakwangi','Luragung','Maleber',
    'Mandirancan','Nusaherang','Pancalang','Pasawahan','Salem','Subang','Sukamulya'
]

# Fungsi untuk menghasilkan data
def generate_data():
    data = []
    for i in range(1, 1201):  # Membuat 1200 entri
        nama = fake.name()
        
        data.append({
            "ID Pasien": generate_pasien_id(i),
            "Nama Lengkap": nama,
            "Umur": random.randint(20, 65),
            "Jenis Kelamin": random.choice(["Laki-laki", "Perempuan"]),
            "Kecamatan": random.choice(kecamatan),
            "Pekerjaan": random.choice(profesi),
            "Nomor Telepon": fake.phone_number(),
            "Email": nama.lower().replace(' ', '.')+'@email.com'
        })
    
    return data

# Memanggil fungsi dan menyimpan data ke file JSON
data_pasien = generate_data()  # Memanggil fungsi untuk menghasilkan data
with open('data_pasien.json', 'w', encoding='utf-8') as file:
    json.dump(data_pasien, file, indent=4, ensure_ascii=False)

print("Data pasien berhasil disimpan ke file 'data_pasien.json'.")