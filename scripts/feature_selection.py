import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# ─────────────────────────────────────────────
# 1. SPARK SESSION SETUP
# ─────────────────────────────────────────────
def make_spark():
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    return (SparkSession.builder
        .appName("Feature Selection Analysis")
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",             access_key)
        .config("spark.hadoop.fs.s3a.secret.key",             secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())

# ─────────────────────────────────────────────
# 2. MAIN ANALYSIS FUNCTION
# ─────────────────────────────────────────────
def main():
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    INPUT_PATH = "s3a://datalake-kelompok2/gold/ml_ready/"
    OUTPUT_PATH     = f"s3a://datalake-kelompok2/gold/feature_selection/"
    PLOT_LOCAL = "plots/correlation_matrix.png"
    
    print(f"[INFO] Loading data from {INPUT_PATH}...")
    df = spark.read.parquet(INPUT_PATH)

    # Definisikan fitur kandidat dan target
    candidate_features = [
        "sales_velocity", "stock_on_hand", "avg_daily_demand", 
        "procurement_lead_time", "supplier_risk", "order_buffer_index", 
        "stock_cover", "inventory_turnover_rate", "log_sales", 
        "demand_to_stock_ratio", "total_sales", "transaction_frequency"
    ]
    target_col = "reorder_point"
    all_cols = candidate_features + [target_col]

    # --- A. Hitung Korelasi menggunakan Spark MLlib ---
    assembler = VectorAssembler(inputCols=all_cols, outputCol="features", handleInvalid="keep")
    v_df = assembler.transform(df).select("features")
    
    matrix = Correlation.corr(v_df, "features").collect()[0][0]
    corr_matrix = matrix.toArray()

    # --- B. Visualisasi dengan Seaborn ---
    corr_df = pd.DataFrame(corr_matrix, index=all_cols, columns=all_cols)
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_df, annot=True, fmt=".2f", cmap='coolwarm', center=0)
    plt.title("Correlation Matrix - Inventory Features")
    plt.tight_layout()
    
    # Simpan lokal dulu
    plt.savefig(PLOT_LOCAL)
    print(f"[✅] Plot berhasil dibuat secara lokal: {PLOT_LOCAL}")

if __name__ == "__main__":
    main()