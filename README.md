# Proyek-BDA-A-Kelompok-2
## Infrastruktur
Proyek ini berjalan di atas Docker dengan layanan:
- **PostgreSQL** ---> Sebagai sumber data transaksional.
- **MinIO** ---> Sebagai Object Storage (Data Lake).
- **MinIO Client (MC)** ---> Untuk konfigurasi otomatis bucket.
## Penyiapan Infrastruktur - Ingestion (Bronze)
1. Clone repositori
  ```
  git clone https://github.com/CantikaZahnaBrilliantoPutri/Proyek-BDA-A-Kelompok-2.git
  ```
2. Persiapan Infrastruktur
  Pastikan Docker Desktop sudah berjalan, kemudian buka terminal di folder proyek dan jalankan:
  ```
  docker compose up -d
  ```
3. Buat file .env dengan menjalankan
  ```
  cp .env.example .env
  ```
4. Buat virtual environment
  ```
  python -m venv venv
  venv\Scripts\activate
  ```
5. Instalansi Library Python
  ```
  pip install -r requirements.txt
  ```
6. Sebelum melakukan ingestion, pastikan database postgres sudah terisi data
  ```
  docker exec -it postgres-kelompok2 psql -U postgres -d postgres -c "SELECT COUNT(*) FROM stock_move;"
  ```
7. Jalankan script python untuk memindahkan data ke Data Lake, yaitu
  ```
  python scripts/ingest_to_datalake.py
  ```
8. Data Lake MinIO
  Setelah script dijalankan, data akan tersimpan di bucket datalake-kelompok2 dengan struktur berikut:
  ```
  raw/stock_transactions.csv (dari Postgres)
  raw/grocery-inventory.csv (dari Local CSV)
  raw/suppliers_info.json (dari Local JSON)
  raw/
  ├── stock_transactions.csv (dari Postgres)
  ├── grocery-inventory.csv (dari Local CSV)
  └── suppliers_info.json (dari Local JSON)
  ```
## Data Cleaning & Pre-Processing (Silver)
1. Jalankan kode berikut untuk memastikan semua service sudah siap dan semua requirement sudah terinstall:
  ```
  docker compose up -d
  venv\Scripts\activate
  pip install -r requirements.txt
  ```
2. Pastikan data sudah di-ingest ke MiniO. Buka [localhost:9000](http://localhost:9001/), pastikan sudah ada folder `raw`. Jika belum, jalankan ingestion terlebih dahulu
3. Masuk ke dalam container `spark-processor` dan buka shell bash dengan menjalankan kode:
  ```
  docker exec -it spark-processor bash
  ```
4. Setelah masuk ke dalam container (tampilan CLI menjadi `root@<container_id>:/app#`), jalankan kode berikut untuk memulai data cleaning dan pre-processing:
  ```
  spark-submit /app/silver_pyspark.py
  ```
5. Setelah proses selesai, buka MiniO ([localhost:9000](http://localhost:9001/)), dan hasil processing tahap silver dapat dilihat di folder `silver` dengan struktur berikut:
  ```
  silver/
  ├── stock_transactions/
  │   ├── _SUCCESS
  │   └── part-00000-***.snappy.parquet
  └── grocery_inventory/
      ├── _SUCCESS
      └── part-00000-***.snappy.parquet
  ```