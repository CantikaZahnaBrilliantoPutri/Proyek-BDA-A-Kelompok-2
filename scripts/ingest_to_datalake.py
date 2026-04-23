import os
import pandas as pd
from sqlalchemy import create_engine
import boto3
from io import BytesIO
from dotenv import load_dotenv

# 1. Load Konfigurasi
load_dotenv()

# 2. Setup Koneksi
# Postgres
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
)

BUCKET_NAME = "datalake-kelompok2"

def upload_to_minio(df, object_name):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=csv_buffer.getvalue())
    print(f"✅ Berhasil upload {object_name} ke MinIO!")

def main():
    # Pastikan bucket ada
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    except:
        pass

    print("🚀 Memulai proses ingestion...")

    # A. Ambil data dari Postgres (SQL)
    print("Reading from Postgres...")
    df_sql = pd.read_sql("SELECT * FROM stock_move", engine)
    upload_to_minio(df_sql, "raw/stock_transactions.csv")

    # B. Ambil data dari CSV
    print("Reading from local CSV...")
    df_csv = pd.read_csv("data/raw/grocery-inventory.csv")
    upload_to_minio(df_csv, "raw/grocery-inventory.csv")

    # C. Ambil data dari JSON
    print("Reading from local JSON...")
    df_json = pd.read_json("data/raw/suppliers_info.json")
    upload_to_minio(df_json, "raw/suppliers_info.json")

    print("\n✨ SEMUA PROSES INGESTION SELESAI!")

if __name__ == "__main__":
    main()