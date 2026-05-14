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

def apply_capping(df, column_name, percentile=0.95):
    """
    Menangani outlier dengan membatasi nilai maksimal berdasarkan percentile.
    """
    # Menggunakan akurasi lebih tinggi (0.001) untuk data ML Ready
    threshold = df.stat.approxQuantile(column_name, [percentile], 0.001)[0]
    
    max_val = df.select(F.max(column_name)).collect()[0][0]
    print(f"Capping {column_name}: Max Original = {max_val}, Threshold P{int(percentile*100)} = {threshold}")
    
    return df.withColumn(column_name, 
        F.when(F.col(column_name) > threshold, threshold)
         .otherwise(F.col(column_name))
    )

def main():
    bucket = os.environ.get("MINIO_BUCKET", "datalake-kelompok2")
    
    # SOURCE: Mengambil data dari GOLD yang sudah di-EDA
    gold_path = f"s3a://{bucket}/gold/ml_ready/"
    # SINK: Output ke folder khusus hasil capping
    capping_output_path = f"s3a://{bucket}/gold/ml_ready_capped/"

    spark = make_spark()

    print(f"Membaca data dari: {gold_path}")
    df = spark.read.parquet(gold_path)

    # Daftar kolom yang memiliki outlier berdasarkan laporan EDA
    # Kita gunakan percentile 0.95 atau 0.99 agar tidak membuang terlalu banyak variansi
    outlier_columns = {
        "procurement_lead_time": 0.95, 
        # "sales_velocity": 0.85,
        "total_sales": 0.99,
        "avg_daily_demand": 0.99,
        "transaction_frequency": 0.99
        # "demand_to_stock_ratio": 0.90
    }

    df_capped = df
    for col_name, p_value in outlier_columns.items():
        if col_name in df.columns:
            df_capped = apply_capping(df_capped, col_name, percentile=p_value)
        else:
            print(f"Warning: Kolom {col_name} tidak ditemukan, melewati...")

    # Simpan hasil
    print(f"Menyimpan data hasil capping ke: {capping_output_path}")
    df_capped.write.mode("overwrite").parquet(capping_output_path)
    
    print("\n--- Ringkasan Statistik Setelah Capping ---")
    df_capped.select(*outlier_columns.keys()).describe().show()

    spark.stop()

if __name__ == "__main__":
    main()