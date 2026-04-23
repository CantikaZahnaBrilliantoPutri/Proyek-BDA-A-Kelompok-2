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
6. Jalankan script python untuk memindahkan data ke Data Lake, yaitu
  ```
  python ingest_to_datalake.py
  ```
7. Data Lake MinIO
  Setelah script dijalankan, data akan tersimpan di bucket datalake-kelompok2 dengan struktur berikut:
  ```
  raw/stock_transactions.csv (dari Postgres)
  raw/grocery-inventory.csv (dari Local CSV)
  raw/suppliers_info.json (dari Local JSON)
  ```
## Data Cleaning & Pre-Processing (Silver)
