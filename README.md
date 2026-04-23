# Proyek-BDA-A-Kelompok-2
## Infrastruktur
Proyek ini berjalan di atas Docker dengan layanan:
- **PostgreSQL** ---> Sebagai sumber data transaksional.
- **MinIO** ---> Sebagai Object Storage (Data Lake).
- **MinIO Client (MC)** ---> Untuk konfigurasi otomatis bucket.
- **Apache Spark (PySpark)** ---> Untuk transformasi Bronze → Silver.
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
  raw/
  ├── stock_transactions.csv (dari Postgres)
  ├── grocery-inventory.csv (dari Local CSV)
  └── suppliers_info.json (dari Local JSON)
  ```
## Data Cleaning & Pre-Processing (Silver)
### Deskripsi Umum Silver Layer
Silver layer bertujuan untuk mengubah data raw (bronze) menjadi data yang lebih bersih, konsisten, dan siap dianalisis/diolah lanjut. Proses ini dijalankan menggunakan `PySpark`.

#### Input (Bronze)
Data dibaca dari MinIO bucket `datalake-kelompok2` pada folder `raw/`:
```
raw/stock_transactions.csv
raw/grocery-inventory.csv
raw/suppliers_info.json
```

#### Transformasi yang dilakukan
1. **Standarisasi nama kolom** agar konsisten dan mudah digunakan untuk analisis
    - Mengubah nama kolom menjadi huruf kecil (lowercase)
    - Menghapus spasi di awal dan akhir
    - Mengganti karakter pemisah seperti spasi / (-) menjadi underscore (_)

2. **Trimming kolom bertipe string**
    - Semua kolom string di-trim untuk menghilangkan whitespace yang tidak perlu

3. **Parsing kolom tanggal/waktu**
    - Kolom yang namanya mengandung kata date, `time`, atau `created` akan diubah menjadi tipe timestamp

4. **Penanganan nilai `null`**
    - Pada dataset inventory, kolom harga akan dicast menjadi numeric, nilai `null` akan diisi menggunakan median (pendekatan *percentile_approx*), lalu dilakukan *Deduplication* (menghapus data duplikat)

5. **Data yang duplikat dihapus**
    - jika ada kolom `id`, deduplikasi berdasarkan `id`
    - jika tidak ada, deduplikasi berdasarkan seluruh baris
    
6. **Pembuatan ID transaksi jika tidak tersedia**
    - Pada dataset transaksi, jika kolom `transaction_id` tidak ada, maka dibuat otomatis menggunakan `uuid()`

#### Output (Silver)
Hasil disimpan kembali ke MinIO dalam format Parquet pada folder `silver/`
```
  silver/
  ├── stock_transactions/
  │   ├── _SUCCESS
  │   └── part-00000-***.snappy.parquet
  └── grocery_inventory/
      ├── _SUCCESS
      └── part-00000-***.snappy.parquet
  ```
> Karena output ditulis oleh Spark, masing-masing folder berisi beberapa file part-*.parquet dan marker _SUCCESS

### Cara Menjalankan Proyek
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
5. Setelah proses selesai, buka MiniO ([localhost:9000](http://localhost:9001/)),  hasil processing tahap silver dapat dilihat di folder `silver`.