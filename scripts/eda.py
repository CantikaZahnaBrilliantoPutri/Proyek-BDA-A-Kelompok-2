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
# from scipy.stats import zscore
from scipy.stats import normaltest, zscore
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


# fungsi terkait file
def read_csv_from_minio(key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        print(f"Error: File {key} tidak ditemukan di bucket {MINIO_BUCKET}")
        exit(1)

def read_json_from_minio(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
    return pd.read_json(BytesIO(obj["Body"].read()))

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


# fungsi normality
def check_normality(s: pd.Series):
    s = s.dropna().astype(float)
    n = len(s)
    
    if n < 8: 
        return "N/A (Too Small)", 0
    
    # Menggunakan D'Agostino's K^2 Test
    # Cocok untuk sampel besar
    stat, p = normaltest(s)
    
    status = "Normal" if p > 0.05 else "Not Normal"
    
    # Catatan: Pada N sangat besar, visualisasi (Histogram/QQ-Plot) 
    # seringkali lebih jujur daripada uji statistik murni.
    return status, p

# def check_normality(s: pd.Series):
#     s = s.dropna().astype(float)
#     if len(s) < 3: return "N/A", 0
#     stat, p = shapiro(s)
#     return ("Normal" if p > 0.05 else "Not Normal"), p


# fungsi null value
def check_missing_values(df: pd.DataFrame):
    # Menghitung jumlah dan persentase missing values per kolom.
    missing_data = df.isnull().sum()
    missing_percent = (df.isnull().sum() / len(df)) * 100
    
    report = []
    for col in df.columns:
        if missing_data[col] > 0:
            report.append([col, missing_data[col], f"{missing_percent[col]:.2f}%"])
    
    return report


# fungsi data kategorikal
def check_categorical_features(df: pd.DataFrame):
    # Menganalisis kolom kategorikal: kardinalitas dan saran encoding.
    cat_cols = df.select_dtypes(include=["object", "string", "category", "bool"]).columns.tolist()
    report = []
    
    for col in cat_cols:
        unique_count = df[col].nunique()
        # Logika sederhana untuk saran encoding
        if unique_count == 2:
            encoding_suggestion = "Binary/Label Encoding"
        elif unique_count <= 10:
            encoding_suggestion = "One-Hot Encoding"
        else:
            encoding_suggestion = "Target/Hash/Frequency Encoding (High Cardinality)"
            
        report.append([col, unique_count, df[col].mode()[0] if not df[col].mode().empty else "N/A", encoding_suggestion])
    
    return report


# fungsi imbalance
def check_imbalance(df: pd.DataFrame, threshold: float = 20.0):
    # Mengecek ketidakseimbangan data pada kolom kategorikal.
    # threshold: selisih persentase minimal antara kategori mayoritas dan minoritas 
    #            untuk dianggap 'imbalanced'.
    cat_cols = df.select_dtypes(include=["object", "string", "category", "bool"]).columns.tolist()
    imbalance_report = []

    for col in cat_cols:
        counts = df[col].value_counts(normalize=True) * 100
        if len(counts) < 2: continue # Abaikan jika hanya 1 kategori
        
        # Ambil persentase tertinggi dan terendah
        max_perc = counts.max()
        min_perc = counts.min()
        diff = max_perc - min_perc
        
        status = "Imbalanced" if diff > threshold else "Balanced"
        
        # Memberikan catatan jika sangat ekstrim (misal mayoritas > 90%)
        note = "High Imbalance" if max_perc > 90 else "-"
        
        imbalance_report.append([col, f"{max_perc:.1f}%", f"{min_perc:.1f}%", status, note])
        
    return imbalance_report


# fungsi plotting
def plot_box(col_series: pd.Series, out_path: str):
    plt.figure(figsize=(6,4))
    sns.boxplot(x=col_series)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def plot_normality(col_series: pd.Series, out_path: str):
    plt.figure(figsize=(6,4))
    sns.histplot(col_series, kde=True, color="skyblue")
    plt.title(f"Distribution: {col_series.name}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def plot_categorical_dist(df: pd.DataFrame, col: str, out_path: str):
    # Membuat bar plot untuk melihat distribusi frekuensi kategori (top 10 jika terlalu banyak).
    plt.figure(figsize=(8, 5))
    # Ambil 10 teratas jika kategori terlalu banyak agar plot tidak berantakan
    order = df[col].value_counts().iloc[:10].index
    # sns.countplot(data=df, y=col, order=order, palette="viridis")
    sns.countplot(data=df, y=col, hue=col, order=order, palette="viridis", legend=False)
    plt.title(f"Top Categories: {col}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def plot_imbalance(df: pd.DataFrame, col: str, out_path: str):
    # Membuat Pie Chart untuk memvisualisasikan imbalance pada kolom kategorikal.
    plt.figure(figsize=(7, 7))
    counts = df[col].value_counts()
    
    # Ambil top 10 jika kategori terlalu banyak agar pie chart tetap terbaca
    if len(counts) > 10:
        counts = counts.head(10)
        plt.title(f"Distribution of {col} (Top 10)")
    else:
        plt.title(f"Distribution of {col}")

    plt.pie(counts, labels=counts.index, autopct='%1.1f%%', startangle=140, 
            colors=sns.color_palette("pastel"))
    plt.axis('equal')  # Agar lingkaran sempurna
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def main():
    output_dir = "eda_output/"
    os.makedirs(output_dir, exist_ok=True)

    folder_prefix = "raw/"
    file_keys = list_files_in_minio(folder_prefix)

    if not file_keys:
        print(f"Tidak ada file ditemukan di folder {folder_prefix}")
        return

    print(f"Ditemukan {len(file_keys)} file. Memulai proses EDA...\n")

    all_outliers = []
    all_normality = []
    all_missing = []
    all_categorical = []
    all_imbalance = []

    for key in file_keys:
        print(f"\nMemproses File {key} ...")
        
        try:
            # Deteksi format file sederhana
            if key.endswith('.csv'):
                df = read_csv_from_minio(key)
            elif key.endswith('.json'):
                df = read_json_from_minio(key)
            else:
                print(f"Skip {key}: Format tidak didukung.")
                continue
                
        except Exception as e:
            print(f"Gagal mengambil data {key}: {e}")
            continue
        
        # cek null value
        missing_report = check_missing_values(df)
        for item in missing_report:
            # item berisi [kolom, jumlah, persen], tambahkan nama file di depan
            all_missing.append([key] + item)

        file_name = os.path.basename(key).split('.')[0]
        
        # cek kategorikal
        cat_report = check_categorical_features(df)
        cat_plot_dir = os.path.join(output_dir, "categorical/", file_name)
        os.makedirs(cat_plot_dir, exist_ok=True)

        for item in cat_report:
            all_categorical.append([key] + item)
            # Plot distribusi kategori
            plot_categorical_dist(df, item[0], os.path.join(cat_plot_dir, f"dist_{item[0]}.png"))

        # proses imbalance
        imbalance_report = check_imbalance(df)
        imb_plot_dir = os.path.join(output_dir, "imbalance/", file_name) # Folder baru
        os.makedirs(imb_plot_dir, exist_ok=True)

        for item in imbalance_report:
            all_imbalance.append([key] + item)
            # Plot Pie Chart (Proporsi/Imbalance)
            # item[0] adalah nama kolom
            plot_imbalance(df, item[0], os.path.join(imb_plot_dir, f"pie_{item[0]}.png"))

        # proses data numerik
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if 'id' in numeric_cols: numeric_cols.remove('id')
        if not numeric_cols:
            print("No numeric columns found in dataset.")
            return
        
        # Setup direktori per file untuk plot
        file_name = os.path.basename(key).split('.')[0]
        outlier_dir = os.path.join(output_dir, "outlier/", file_name)
        normality_dir = os.path.join(output_dir, "normality/", file_name)
        os.makedirs(outlier_dir, exist_ok=True)
        os.makedirs(normality_dir, exist_ok=True)
        
        # print("\n" + "="*50)
        # print("       EXPLORATORY DATA ANALYSIS REPORT")
        # print("="*50)

        for col in numeric_cols:
            # 1. Hitung Statistik & Outlier
            stats = compute_stats(df[col])
            out_z = detect_outliers_z(df[col])
            out_iqr = detect_outliers_iqr(df[col])
            
            # Masukkan ke list untuk tabel
            all_outliers.append([
                key, col, stats['min'], stats['q1'], stats['median'], 
                stats['mean'], stats['q3'], stats['max'], 
                len(out_iqr), len(out_z)
            ])

            # 2. Hitung Normalitas
            norm_status, p_val = check_normality(df[col])
            all_normality.append([
                key, col, stats['mean'], stats['median'], 
                norm_status, f"{p_val:.4f}"
            ])

            # Simpan Plot PNG
            plot_box(df[col].dropna(), os.path.join(outlier_dir, f"boxplot_{col}.png"))
            plot_normality(df[col].dropna(), os.path.join(normality_dir, f"dist_{col}.png"))

    # --- BAGIAN PELAPORAN TERPUSAT ---
    
    print("\n" + "="*80)
    print("                 GLOBAL EXPLORATORY DATA ANALYSIS REPORT")
    print("="*80)


    print("\n[1. OUTLIER ANALYSIS]")
    headers_out = ["File Source", "Column", "Min", "Q1", "Median", "Mean", "Q3", "Max", "Out(IQR)", "Out(Z)"]
    if all_outliers:
        print(tabulate(all_outliers, headers=headers_out, tablefmt="fancy_grid", floatfmt=".2f"))
    else:
        print("Tidak ada data outlier untuk ditampilkan.")


    print("\n" + "-"*80)


    print("\n[2. NORMALITY TEST (D'Agostino K^2)]")
    headers_norm = ["File Source", "Column", "Mean", "Median", "Is Normal?", "P-Value"]
    if all_normality:
        print(tabulate(all_normality, headers=headers_norm, tablefmt="fancy_grid", floatfmt=".2f"))
    else:
        print("Tidak ada data normalitas untuk ditampilkan.")


    print("\n" + "-"*80)


    print("\n[3. NULL VALUE ANALYSIS]")
    headers_missing = ["File Source", "Column", "Null Count", "Percentage"]
    if all_missing:
        print(tabulate(all_missing, headers=headers_missing, tablefmt="fancy_grid"))
    else:
        print("Bagus! Tidak ditemukan missing values pada semua file.")
    

    print("\n" + "-"*80)


    print("\n[4. CATEGORICAL & ENCODING ANALYSIS]")
    headers_cat = ["File Source", "Column", "Unique Values", "Most Frequent", "Encoding Suggestion"]
    if all_categorical:
        print(tabulate(all_categorical, headers=headers_cat, tablefmt="fancy_grid"))
    else:
        print("Tidak ada kolom kategorikal ditemukan.")


    print("\n" + "-"*80)


    print("\n[5. CLASS IMBALANCE ANALYSIS (Potential Targets)]")
    headers_imb = ["File Source", "Column", "Majority (%)", "Minority (%)", "Status", "Note"]
    if all_imbalance:
        print(tabulate(all_imbalance, headers=headers_imb, tablefmt="fancy_grid"))
        print("\n> Tip: Kolom dengan status 'Imbalanced' atau 'High Imbalance' perlu perhatian khusus jika dijadikan target model (misal: perlu SMOTE atau Class Weight).")
    else:
        print("Tidak ada kolom kategorikal yang cukup untuk dianalisis imbalance-nya.")


    print("\n" + "="*80)
    print(f"[SELESAI] Semua plot disimpan secara terpisah di folder: {output_dir}")
    print("="*80)

    # print("\n[SELESAI] Semua file telah diproses.")
    # print("=" * 50)
if __name__ == "__main__":
    main()