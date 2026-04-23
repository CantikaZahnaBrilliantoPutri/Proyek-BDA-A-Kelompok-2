import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------- Spark session + MinIO (S3A) ----------
def make_spark(app_name: str = "silver-layer"):
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    spark = (
        SparkSession.builder
        .appName(app_name)
        # S3A config for MinIO
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # stability options (helpful with some S3 compatibles)
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate()
    )
    return spark


# ---------- Helpers ----------
def normalize_colname(c: str) -> str:
    # simple snake_case-ish normalization
    return (
        c.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def normalize_columns(df):
    new_cols = [normalize_colname(c) for c in df.columns]
    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def trim_string_columns(df):
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def parse_datetime_columns_by_name(df):
    # heuristic similar to pandas code: columns with date/time/created
    for name, dtype in df.dtypes:
        lowered = name.lower()
        if any(k in lowered for k in ("date", "time", "created")):
            # Try parsing as timestamp; if already timestamp/date, skip
            if dtype == "string":
                df = df.withColumn(name, F.to_timestamp(F.col(name)))
    return df


# ---------- Cleaning transforms ----------
def clean_stock_transactions(df):
    df = normalize_columns(df)
    df = trim_string_columns(df)
    df = parse_datetime_columns_by_name(df)

    # quantity
    if "quantity" in df.columns:
        df = df.withColumn("quantity", F.coalesce(F.col("quantity").cast("double"), F.lit(0.0)))

    # ensure transaction_id
    if "transaction_id" not in df.columns:
        df = df.withColumn("transaction_id", F.expr("uuid()"))

    # dedup
    if "id" in df.columns:
        df = df.dropDuplicates(["id"])
    else:
        df = df.dropDuplicates()

    return df


def clean_inventory(df):
    df = normalize_columns(df)
    df = trim_string_columns(df)

    # price columns
    for candidate in ("price", "unit_price", "cost"):
        if candidate in df.columns:
            df = df.withColumn(candidate, F.col(candidate).cast("double"))
            # fill null with median-like approximation using percentile_approx
            median = df.select(F.expr(f"percentile_approx({candidate}, 0.5)").alias("med")).collect()[0]["med"]
            df = df.withColumn(candidate, F.coalesce(F.col(candidate), F.lit(median)))
            break

    df = df.dropDuplicates()
    return df


def clean_suppliers(df):
    # For JSON, Spark already flattens top-level keys. Nested keys remain structs/arrays.
    # We'll normalize names and keep it as-is; optionally you can flatten structs later.
    df = normalize_columns(df)
    df = trim_string_columns(df)

    if "supplier_id" not in df.columns and "id" in df.columns:
        df = df.withColumnRenamed("id", "supplier_id")

    df = df.dropDuplicates()
    return df


# ---------- Main pipeline ----------
def main():
    bucket = os.environ.get("MINIO_BUCKET", "datalake-kelompok2")

    bronze_stock = f"s3a://{bucket}/raw/stock_transactions.csv"
    bronze_inventory = f"s3a://{bucket}/raw/grocery-inventory.csv"
    bronze_suppliers = f"s3a://{bucket}/raw/suppliers_info.json"

    silver_stock = f"s3a://{bucket}/silver/stock_transactions/"
    silver_inventory = f"s3a://{bucket}/silver/grocery_inventory/"
    silver_suppliers = f"s3a://{bucket}/silver/suppliers/"

    spark = make_spark()

    # 1) stock_transactions (CSV)
    df_stock = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_stock)
    df_stock_clean = clean_stock_transactions(df_stock)
    df_stock_clean.write.mode("overwrite").parquet(silver_stock)

    # 2) grocery inventory (CSV)
    df_inv = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_inventory)
    df_inv_clean = clean_inventory(df_inv)
    df_inv_clean.write.mode("overwrite").parquet(silver_inventory)

    # 3) suppliers (JSON)
    df_sup = spark.read.option("multiLine", "true").json(bronze_suppliers)
    df_sup_clean = clean_suppliers(df_sup)
    df_sup_clean.write.mode("overwrite").parquet(silver_suppliers)

    # basic quality logs
    print("Silver counts:")
    print("stock_transactions:", df_stock_clean.count())
    print("grocery_inventory:", df_inv_clean.count())
    print("suppliers:", df_sup_clean.count())

    spark.stop()


if __name__ == "__main__":
    main()