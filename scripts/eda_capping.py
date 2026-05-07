import os
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F

def make_spark(app_name: str = "eda-capping-processing"):
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark

def apply_capping(df, column_name, percentile=0.90):
    """
    Fungsi untuk menjinakkan outlier dengan Capping (Winsorization).
    Nilai di atas percentile (misal 99%) akan dipaksa menjadi nilai percentile tersebut.
    """
    # Mencari nilai ambang batas (threshold) menggunakan approxQuantile
    threshold = df.stat.approxQuantile(column_name, [percentile], 0.01)[0]
    
    print(f"DEBUG: Threshold untuk {column_name} (P{int(percentile*100)}) adalah: {threshold}")
    
    # Terapkan Capping: Jika nilai > threshold, ganti dengan threshold
    return df.withColumn(column_name, 
        F.when(F.col(column_name) > threshold, threshold)
         .otherwise(F.col(column_name))
    )

def main():
    bucket = os.environ.get("MINIO_BUCKET", "datalake-kelompok2")
    
    # SOURCE: Membaca dari data SILVER yang sudah bersih tipe datanya
    silver_stock_path = f"s3a://{bucket}/silver/stock_transactions/"
    
    # SINK: Menyimpan ke folder baru
    capping_output_path = f"s3a://{bucket}/eda_capping/stock_transactions/"

    spark = make_spark()

    # 1. Load data Silver
    print(f"Membaca data dari: {silver_stock_path}")
    df_stock = spark.read.parquet(silver_stock_path)

    # 2. Proses Capping pada quantity_change
    if "quantity_change" in df_stock.columns:
        df_capped = apply_capping(df_stock, "quantity_change", percentile=0.90)
        
        # 3. Simpan hasil ke folder baru
        print(f"Menyimpan data hasil capping ke: {capping_output_path}")
        df_capped.write.mode("overwrite").parquet(capping_output_path)
        
        print("Proses Capping Selesai ✅")
        df_capped.select("quantity_change").describe().show()
    else:
        print("Error: Kolom quantity_change tidak ditemukan!")

    spark.stop()

if __name__ == "__main__":
    main()