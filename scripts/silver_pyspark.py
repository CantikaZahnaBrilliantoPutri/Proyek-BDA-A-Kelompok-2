import os # untuk membaca env
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql import types as T

# fungsi untuk membuat spark session, konfigurasi MiniO S3A
def make_spark(app_name: str = "silver-layer-processing"):
    # inisialisasi variabel
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    # membuat spark session
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Konfigurasi S3A untuk MinIO 
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate()
    )
    return spark


# fungsi untuk normalisasi nama kolom
def normalize_colname(c: str) -> str:
    return (
        c.strip() # hapus whitespace di awal dan akhir
        .lower() # jadi lower case
        .replace(" ", "_") # space jadi underscore
        .replace("-", "_") # strip jadi underscore
        .replace("/", "_") # slash jadi underscore
    )

# fungsi menerima dataframe spark dan mengembalikan dataframe yang kolomnya sudah dinormalisasi
def normalize_columns(df):
    # memanggil fungsi normalisasi nama kolom yang dibuat di atas
    # membuat daftar nama kolom baru yang sudah dinormalisasi
    new_cols = [normalize_colname(c) for c in df.columns]
    # ganti nama kolom lama dengan yang baru
    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df

# fungsi untuk hilangkan whitespace sebelum dan sesudah kolom
def trim_string_columns(df):
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df

# fungsi untuk normalisasi kolom datetime
def parse_datetime_columns_by_name(df):
    for name, dtype in df.dtypes:
        lowered = name.lower()
        # kalau nama kolom mengandung date atau time atau created
        if any(k in lowered for k in ("date", "time", "created")):
            # kalay tipenya string, coba parse jadi timestamp dengan to_timestamp
            if dtype == "string":
                df = df.withColumn(name, F.to_timestamp(F.col(name)))
    return df


# fungsi untuk cleaning stock transaction (sql)
def clean_stock_transactions(df):
    # panggil fungsi yang sudah dibuat di atas
    df = normalize_columns(df) # normalisasi nama kolom
    df = trim_string_columns(df) # trim kolom string
    df = parse_datetime_columns_by_name(df) # parse kolom datetime

    # kalau ada kolom quantity
    if "quantity" in df.columns:
        # parse ke tipe data double
        # kalau hasilnya null, isi dengan 0.0
        df = df.withColumn("quantity", F.coalesce(F.col("quantity").cast("double"), F.lit(0.0)))

    # buat kolom transaction_id yang nilainya adalah UUID
    if "transaction_id" not in df.columns:
        df = df.withColumn("transaction_id", F.expr("uuid()"))

    # kalau ada kolom id, buang baris duplikat berdasarkan id
    # kalau tidak ada, buang baris duplikat berdasarkan baris full
    if "id" in df.columns:
        df = df.dropDuplicates(["id"])
    else:
        df = df.dropDuplicates()

    return df

# fungsi untuk cleaning grocery inventory (csv)
def clean_inventory(df):
    # panggil fungsi yang sudah dibuat di atas
    df = normalize_columns(df)
    df = trim_string_columns(df)

    # definisikan kolom harga
    for candidate in ("price", "unit_price", "cost"):
        if candidate in df.columns:
            # cast kolom harga jadi double
            df = df.withColumn(candidate, F.col(candidate).cast("double"))
            # hitung median (percentile_approx) untuk mengisi null
            median = df.select(F.expr(f"percentile_approx({candidate}, 0.5)").alias("med")).collect()[0]["med"]
            # kalau null, isi dengan median
            df = df.withColumn(candidate, F.coalesce(F.col(candidate), F.lit(median)))
            break
    # hilangkan baris duplikat
    df = df.dropDuplicates()
    return df

# fungsi untuk cleaning suppliers (json)
def clean_suppliers(df):
    # panggil fungsi yang sudah dibuat di atas
    df = normalize_columns(df)
    df = trim_string_columns(df)

    # kalau tidak ada kolom supplier_id tapi ada id, rename id menjadi supplier_id supaya konsisten
    if "supplier_id" not in df.columns and "id" in df.columns:
        df = df.withColumnRenamed("id", "supplier_id")

    # hilangkan baris duplikat
    df = df.dropDuplicates()
    return df


# fungsi / pipeline utama
def main():
    # ambil nama bucket di minio
    bucket = os.environ.get("MINIO_BUCKET", "datalake-kelompok2")

    # membuat path input di MinIO menggunakan skema s3a://
    bronze_stock = f"s3a://{bucket}/raw/stock_transactions.csv"
    bronze_inventory = f"s3a://{bucket}/raw/grocery-inventory.csv"
    bronze_suppliers = f"s3a://{bucket}/raw/suppliers_info.json"

    # membuat path output
    silver_stock = f"s3a://{bucket}/silver/stock_transactions/"
    silver_inventory = f"s3a://{bucket}/silver/grocery_inventory/"
    silver_suppliers = f"s3a://{bucket}/silver/suppliers/"

    # membuat spark session
    spark = make_spark()

    # proses file stock (csv)
    # baris pertama dianggap header
    # inferSchema = perkirakan tipe data kolom
    df_stock = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_stock)
    # panggil fungsi untuk cleaning
    df_stock_clean = clean_stock_transactions(df_stock)
    # output sebelumnya akan di overwrite
    df_stock_clean.write.mode("overwrite").parquet(silver_stock)

    # proses file grocery
    # kurang lebih prosesnya sama
    df_inv = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_inventory)
    df_inv_clean = clean_inventory(df_inv)
    df_inv_clean.write.mode("overwrite").parquet(silver_inventory)

    # proses file supplier
    # multiline=true karena 1 JSON terdiri dari banyak baris
    df_sup = spark.read.option("multiLine", "true").json(bronze_suppliers)
    df_sup_clean = clean_suppliers(df_sup)
    df_sup_clean.write.mode("overwrite").parquet(silver_suppliers)

    # menghitung dan menampilkan jumlah baris hasil cleaning
    print("Silver counts:")
    print("stock_transactions:", df_stock_clean.count())
    print("grocery_inventory:", df_inv_clean.count())
    print("suppliers:", df_sup_clean.count())

    # menghentikan spark session
    spark.stop()

# entry point
# jika file ini dijalankan, maka fungsi main() dieksekusi
if __name__ == "__main__":
    main()