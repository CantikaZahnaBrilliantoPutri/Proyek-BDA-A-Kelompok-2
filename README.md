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
6. Sebelum melakukan ingestion, pastikan database postgres sudah terisi data (`count` tidak 0)
  ```
  docker exec -it postgres-kelompok2 psql -U postgres -d postgres -c "SELECT COUNT(*) FROM stock_move;"
  ```
7. Jalankan script python untuk memindahkan data ke Data Lake, yaitu
  ```
  python scripts/ingest_to_datalake.py
  ```
8. Data Lake MinIO
  Setelah script dijalankan, data akan tersimpan di bucket `datalake-kelompok2` dengan struktur berikut:
  ```
  raw/
  ├── stock_transactions.csv (dari Postgres)
  ├── grocery-inventory.csv (dari Local CSV)
  └── suppliers_info.json (dari Local JSON)
  ```
## Exploratory Data Analysis (EDA)
Exploratory Data Analysis meliputi 5 jenis pemeriksaan:
1. Outlier Analysis -> Mengidentifikasi outlier dari semua sumber data
2. Normality Test (D'Agostino K^2) -> Melihat apakah data terdistribusi normal
3. Null Value Analysis -> Memeriksa apakah terdapat nilai null pada data
4. Categorical & Encoding Analysis -> Memeriksa kolom kategorikal dan menentukan jenis encoding yang sesuai
5. Class Imbalance Analysis -> Memeriksa imbalance data pada kolom target modelling

Hasil dari kelima pemeriksaan tersebut adalah sebagai berikut:
- Outlier Analysis -> Outlier hanya ada di `raw/stock_transactions.csv` kolom `quantity_change` 
- Normality Test -> Semua data tidak terdistribusi normal
- Null Value -> Tidak ada `null` value di kolom manapun
- Categorical & Encoding -> Terdapat 18 kolom kategorikal, semua kolom telah diberi rekomendasi encoding
- Imbalance -> Data imbalance hanya ada pada kolom `catagory` dan `transaction_type`

Grafik plotting EDA dapat dilihat di folder `eda_output`

### Tindak Lanjut EDA
- Berdasarkan Outlier Analysis IQR, ditemukan sangat banyak outlier pada kolom `quantity_change`. Namun, setelah diverifikasi, nilai-nilai tersebut adalah transaksi stok masuk yang valid secara bisnis. Penanganan outlier akan dilakukan dengan metode capping.

- Meskipun data tidak berdistribusi normal, data ini juga tidak perlu dinormalisasi karena Random Forest tidak mensyaratkan data untuk bersifat normal.

- Terdapat beberapa kolom yang seharusnya bukan kategorikal tetapi terbaca sebagai kolom kategorikal, misalnya kolom `unit_price` dan `receive_date`. Oleh karena itu, kolom-kolom ini akan dicasting ke tipe yang sesuai pada preprocessing silver layer.

- Penanganan data imbalance akan dilakukan sebelum modelling.


### Cara Menjalankan
Jalankan kode berikut di terminal:
```bash
docker-compose run --rm python-eda python scripts/eda.py
```
Setelah proses selesai, akan muncul keterangan `[SELESAI] Semua plot disimpan secara terpisah di folder: eda_output/`

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
1. **Standarisasi/Normalisasi nama kolom** agar konsisten dan mudah digunakan untuk analisis
    - Mengubah nama kolom menjadi huruf kecil (lowercase)
    - Menghapus spasi di awal dan akhir
    - Mengganti karakter pemisah seperti spasi / (-) menjadi underscore (_)

2. **Trimming kolom bertipe string**
    - Semua kolom string di-trim untuk menghilangkan whitespace yang tidak perlu

3. **Parsing kolom tanggal/waktu**
    - Kolom yang namanya mengandung kata date, `time`, atau `created` akan diubah menjadi tipe timestamp

4. **Penghapusan simbol `$` dan `%`**
    - Berdasarkan hasil EDA, kolom `percentage` dan `unit_price` dari data `Grocery_Inventory` terbaca sebagai kolom kategorikal karena memiliki simbol `$` dan `%`
    - Simbol ini dihapus dengan menggunakan fungsi `regexp_replace`, sehingga kolom `percentage` dan `unit_price` dapat terbaca sebagai kolom numerik

5. **Penanganan nilai `null`**
    - Pada dataset inventory, kolom harga akan dicast menjadi numeric, nilai `null` akan diisi menggunakan median (pendekatan *percentile_approx*), lalu dilakukan *Deduplication* (menghapus data duplikat)
    - Pada dataset transaksi, kolom `quantity` akan diubah tipe datanya menjadi double. Jika menghasilkan nilai `null`, maka nilainya akan di-set menjadi `0.0`

6. **Data yang duplikat dihapus**
    - jika ada kolom `id`, deduplikasi berdasarkan `id`
    - jika tidak ada, deduplikasi berdasarkan seluruh baris
    
7. **Pembuatan ID transaksi jika tidak tersedia**
    - Pada dataset transaksi, jika kolom `transaction_id` tidak ada, maka dibuat otomatis menggunakan `uuid()`

8. **Penanganan outlier dengan metode *Capping*/*Winsorization***
    - Hasil EDA menunjukkan ada `quantity_change` yang nilainya sangat besar (outlier), sehingga akan mengacaukan rata-rata. Karena di Gold Layer akan menghitung `sales_velocity` (rata-rata penjualan), maka outlier ini harus ditangani agar model Random Forest tidak memberikan prediksi `reorder_point` yang terlalu tinggi.
    - Metode capping dipilih untuk mempersempit penyebaran data tanpa menghilangkan data penting di kolom `quantity_change`


#### Output (Silver)
Hasil disimpan kembali ke MinIO dalam format Parquet pada folder `silver/`
```
  silver/
  ├── stock_transactions/
  │   ├── _SUCCESS
  │   └── part-00000-***.snappy.parquet
  └── grocery_inventory/
  │   ├── _SUCCESS
  │   └── part-00000-***.snappy.parquet
  └── suppliers/
      ├── _SUCCESS
      └── part-00000-***.snappy.parquet
  ```
> Karena output ditulis oleh Spark, masing-masing folder berisi beberapa file part-*.parquet dan marker _SUCCESS

Hasil dari metode Capping disimpan di folder terpisah, yaitu folder `eda_capping` di MiniO
```
  eda_capping/
  └── stock_transactions/
      ├── _SUCCESS
      └── part-00000-***.snappy.parquet
  ```

### Cara Menjalankan Proyek (Preprocessing)
1. Jalankan kode berikut untuk memastikan semua service sudah siap dan semua requirement sudah terinstall:
  ```bash
  docker compose up -d
  venv\Scripts\activate
  pip install -r requirements.txt
  ```
2. Pastikan data sudah di-ingest ke bucket `datalake-kelompok2` di MiniO. Buka [localhost:9000](http://localhost:9001/), pastikan sudah ada folder `raw` di dalam bucket. Jika belum, jalankan ingestion terlebih dahulu
3. Masuk ke dalam container `spark-processor` dan memulai data cleaning dan pre-processing:
  ```bash
  docker exec -it spark-processor spark-submit /app/scripts/silver_pyspark.py
  ```
4. Setelah muncul baris `s3a-file-system metrics system shutdown complete`, proses telah selesai. Buka/refresh MiniO ([localhost:9000](http://localhost:9001/)), hasil processing tahap silver dapat dilihat di folder `silver`.

### Cara Menjalankan Proyek (Capping)
Jalankan kode berikut:
```bash
docker exec -it spark-processor spark-submit /app/scripts/eda_capping.py     
```
Hasil capping disimpan di MiniO di folder `eda_capping`.

Untuk memverifikasi hasil capping, jalankan kode berikut:
```bash
docker-compose run --rm python-eda python scripts/verifikasi_hasil_capping.py
```

Hasil verifikasi dapat dilihat di terminal, bandingkan output hasil capping dengan output EDA. Setelah capping, nilai MAX akan turun dan outlier berkurang. Plotting dari hasil capping dapat dilihat di folder proyek `verifikasi_capping_output`.

---

## Data Aggregation & Feature Engineering (Gold)

### Deskripsi Umum Gold Layer

Gold layer bertujuan untuk menghasilkan dataset yang siap digunakan untuk machine learning dengan melakukan:

* Aggregasi data transaksi
* Penggabungan antar tabel (inventory, transaksi, supplier)
* Feature engineering
* Penentuan target model (regresi & klasifikasi)

#### Input (Silver)

Data dibaca dari MinIO bucket `datalake-kelompok2` pada folder `silver/`:

```
silver/stock_transactions/
silver/grocery_inventory/
silver/suppliers/
```

#### Transformasi yang dilakukan

1. **Aggregasi Transaksi**

   * `sales_velocity`: total unit terjual dalam 30 hari terakhir
   * `total_sales`: total unit terjual sepanjang waktu
   * `transaction_frequency`: jumlah transaksi penjualan

2. **Join Antar Tabel**

   * Menggabungkan `grocery_inventory`, `stock_transactions`, dan `suppliers`
   * Join berdasarkan `product_id` dan `supplier_id`

3. **Feature Engineering**
   Fitur yang dihasilkan antara lain:

   * `sales_velocity`
   * `stock_on_hand`
   * `avg_daily_demand`
   * `procurement_lead_time`
   * `supplier_risk`
   * `order_buffer_index`
   * `stock_cover`
   * `inventory_turnover_rate`
   * `log_sales`
   * `demand_to_stock_ratio`

4. **Penentuan Target**

   * **Regresi**: `reorder_point` (ROP optimal hasil kalkulasi)
   * **Klasifikasi**: `stockout_risk`

     * 1 = berisiko stockout
     * 0 = aman

#### Output (Gold)

Hasil disimpan ke dalam MinIO pada folder `gold/`:

```
gold/
├── features/
├── aggregates/
└── ml_ready/
```

* `features/` → hanya fitur (X)
* `aggregates/` → ringkasan bisnis
* `ml_ready/` → dataset final untuk modeling (X + Y)

---

### Cara Menjalankan Gold Layer

1. Masuk ke dalam container Spark:

```
docker exec -it spark-processor bash
```

2. Jalankan script Gold:

```
spark-submit /app/scripts/gold_pyspark.py
```

3. Tunggu hingga proses selesai, ditandai dengan:

```
[FINISH] GOLD layer completed ✅✅✅
```

4. Cek hasil di MinIO:

* Buka [http://localhost:9001/](http://localhost:9001/)
* Masuk ke bucket `datalake-kelompok2`
* Pastikan folder `gold/` sudah terisi

---

## Modeling (Machine Learning)

### Deskripsi Umum Modeling

Tahap ini menggunakan dataset `ml_ready` dari Gold untuk membangun dua model:

1. **Random Forest Regressor**

   * Tujuan: memprediksi `reorder_point` optimal

2. **Random Forest Classifier**

   * Tujuan: memprediksi `stockout_risk`

#### Input

```
s3a://datalake-kelompok2/gold/ml_ready/
```

#### Output

```
s3a://datalake-kelompok2/gold/modeling_results/
```

Serta file visualisasi lokal:

```
plots/
├── feature_importance.png
└── confusion_matrix.png
```

---

### Proses Modeling

1. Data preparation (casting, handle null, vector assembler)
2. Train-test split (80:20)
3. Training model:

   * Random Forest Regressor
   * Random Forest Classifier
4. Evaluasi model:

   * Regresi: MAE, RMSE, R²
   * Klasifikasi: AUC, Accuracy, Precision, Recall, F1
5. Visualisasi:

   * Feature importance
   * Confusion matrix
6. Generate output:

   * Prediksi ROP optimal
   * Probabilitas stockout
   * Rekomendasi bisnis

---

### Cara Menjalankan Modeling

1. Pastikan masih berada di dalam container Spark:

```
docker exec -it spark-processor bash
```

2. Jalankan script modeling:

```
spark-submit /app/scripts/modeling.py
```

3. Tunggu hingga proses selesai, ditandai dengan:

```
[FINISH] Modeling Pipeline selesai ✅
```

---

### Hasil Akhir

#### 1. Dataset Hasil Modeling

Tersimpan di:

```
gold/modeling_results/
```

Berisi:

* `optimal_rop_pred`
* `stockout_prediction`
* `stockout_probability`
* `recommendation`

#### 2. Visualisasi

Tersimpan di folder lokal dalam container:

```
plots/
```

Isi:

* `feature_importance.png`
* `confusion_matrix.png`

#### 3. Insight Tambahan

Script juga menampilkan:

* Top 10 produk dengan risiko stockout tertinggi
* Evaluasi performa model

---

## Ringkasan Alur Pipeline

```
PostgreSQL / CSV / JSON
        ↓
Bronze (raw)
        ↓
Silver (cleaned & standardized)
        ↓
Gold (features + target ML)
        ↓
Modeling (RF Regressor & Classifier)
        ↓
Output:
  - Dataset hasil prediksi
  - Visualisasi
  - Insight bisnis
```