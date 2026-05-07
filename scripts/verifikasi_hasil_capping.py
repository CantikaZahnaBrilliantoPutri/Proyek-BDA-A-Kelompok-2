from botocore.config import Config

from dotenv import load_dotenv
import os
import boto3
from io import BytesIO
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import zscore
from tabulate import tabulate

load_dotenv()  # baca .env jika ada

# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-kelompok2:9000")  # in-container default
MINIO_ENDPOINT = "minio-kelompok2:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake-kelompok2")

s3_config = Config(
    signature_version="s3v4",
    s3={"addressing_style": "path"}
)

s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=s3_config,
    region_name="us-east-1"
)

def list_files_in_minio(prefix: str):
    """Mengambil semua key file dalam folder/prefix tertentu."""
    try:
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        # Filter hanya file (abaikan folder itu sendiri) dan pastikan ada isinya
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/')]
        return []
    except Exception as e:
        print(f"Error listing files: {e}")
        return []
    
def read_parquet_from_minio(key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))
    except Exception as e:
        print(f"Error reading Parquet {key}: {e}")
        return pd.DataFrame()
    
# fungsi statistik & outlier
def compute_stats(s: pd.Series):
    s = s.dropna().astype(float)
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "q1": float(q1),
        "median": float(s.median()),
        "mean": float(s.mean()),
        "q3": float(q3),
        "max": float(s.max()),
        "iqr": float(iqr),
        "lower_fence": float(q1 - 1.5 * iqr),
        "upper_fence": float(q3 + 1.5 * iqr),
    }

def detect_outliers_z(s: pd.Series, thresh: float = 3.0):
    s = s.dropna().astype(float)
    zs = zscore(s)
    return s[abs(zs) > thresh]

def detect_outliers_iqr(s: pd.Series, k: float = 1.5):
    s = s.dropna().astype(float)
    q1 = s.quantile(0.25); q3 = s.quantile(0.75); iqr = q3 - q1
    lower, upper = q1 - k * iqr, q3 + k * iqr
    return s[(s < lower) | (s > upper)]

def plot_box(col_series: pd.Series, out_path: str):
    plt.figure(figsize=(6,4))
    sns.boxplot(x=col_series)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def main():
    output_dir = "verifikasi_capping_output/" # Beri nama folder berbeda untuk verifikasi
    os.makedirs(output_dir, exist_ok=True)

    # UBAH DISINI: Arahkan ke folder hasil capping
    folder_prefix = "eda_capping/stock_transactions/" 
    file_keys = list_files_in_minio(folder_prefix)

    # Filter hanya file parquet (Spark sering menghasilkan file _SUCCESS, abaikan itu)
    file_keys = [k for k in file_keys if k.endswith('.parquet')]

    if not file_keys:
        print(f"Tidak ada file parquet ditemukan di folder {folder_prefix}")
        return

    print(f"Memverifikasi {len(file_keys)} file hasil capping...\n")

    all_outliers = []

    for key in file_keys:
        print(f"Memproses verifikasi: {key}")
        
        # GUNAKAN PEMBACA PARQUET
        df = read_parquet_from_minio(key)
        
        if df.empty: continue

        # proses data numerik
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if 'id' in numeric_cols: numeric_cols.remove('id')
        if not numeric_cols:
            print("No numeric columns found in dataset.")
            return
        
        # Setup direktori per file untuk plot
        file_name = os.path.basename(key).split('.')[0]
        outlier_dir = os.path.join(output_dir, file_name)
        os.makedirs(outlier_dir, exist_ok=True)

        for col in numeric_cols:
            # 1. Hitung Statistik & Outlier
            stats = compute_stats(df[col])
            out_z = detect_outliers_z(df[col])
            out_iqr = detect_outliers_iqr(df[col])
            
            # Masukkan ke list untuk tabel
            all_outliers.append([
                col, stats['min'], stats['q1'], stats['median'], 
                stats['mean'], stats['q3'], stats['max'], 
                len(out_iqr), len(out_z)
            ])

            plot_box(df[col].dropna(), os.path.join(outlier_dir, f"boxplot_{col}.png"))

    print("\n" + "="*80)
    print("                 GLOBAL EXPLORATORY DATA ANALYSIS REPORT")
    print("="*80)

    print("\n[1. OUTLIER ANALYSIS]")
    headers_out = ["Column", "Min", "Q1", "Median", "Mean", "Q3", "Max", "Out(IQR)", "Out(Z)"]
    if all_outliers:
        print(tabulate(all_outliers, headers=headers_out, tablefmt="fancy_grid", floatfmt=".2f"))
    else:
        print("Tidak ada data outlier untuk ditampilkan.")


    print("\n" + "="*80)
    print(f"[SELESAI] Semua plot disimpan secara terpisah di folder: {output_dir}")
    print("="*80)

    # print("\n[SELESAI] Semua file telah diproses.")
    # print("=" * 50)
if __name__ == "__main__":
    main()