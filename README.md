# Proyek-BDA-A-Kelompok-2
1. Data Ingestion
   # Infrastruktur
   Proyek ini berjalan di atas Docker dengan layanan:
   a. **PostgreSQL** ---> Sebagai sumber data transaksional.
   b. **MinIO** ---> Sebagai Object Storage (Data Lake).
   c. **MinIO Client (MC)** ---> Untuk konfigurasi otomatis bucket.
   # Cara Menjalankan Proyek
   - Persiapan Infrastruktur
     Pastikan Docker Desktop sudah berjalan, kemudian buka terminal di folder proyek dan jalankan:
        ```bash
        docker compose up -d

   - Instalansi Library Python
     Gunakan Virtual Environment dan instal dependensi yang dibutuhkan dengan kode:
     pip install -r requirements.txt
     
   - Menjalankan Pipeline
     Jalankan script python untuk memindahkan data ke Data Lake, yaitu
     python ingest_to_datalake.py
     
   - Data Lake MinIO
     Setelah script dijalankan, data akan tersimpan di bucket datalake-kelompok2 dengan struktur berikut:
     raw/stock_transactions.csv (dari Postgres)
     raw/grocery-inventory.csv (dari Local CSV)
     raw/suppliers_info.json (dari Local JSON)
