import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# fungsi untuk membuat spark session, konfigurasi MinIO S3A
def make_spark(app_name: str = "gold-layer-processing"):
    # inisialisasi variabel
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    # membuat spark session
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Konfigurasi S3A untuk MinIO
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",             access_key)
        .config("spark.hadoop.fs.s3a.secret.key",             secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.maximum",     "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum",       "10")
        .getOrCreate()
    )
    return spark

# ── LOGGING ───────────────────────────────────────────────────────────────────

# counter untuk melacak progress step
_step_done  = 0
_step_total = 0

def log_start(step: str):
    print(f"[START] {step}")

def log_success(step: str):
    global _step_done
    _step_done += 1
    print(f"[✅ SUCCESS] {step}")
    print(f"Progress: {_step_done}/{_step_total} steps completed")

def log_failed(step: str, error: Exception):
    print(f"[❌ FAILED] {step} failed: {error}")
    raise error

def log_feature(name: str, idx: int, total: int = 8):
    print(f"[✅ SUCCESS] Feature {name} created ({idx}/{total})")

# ── READ ──────────────────────────────────────────────────────────────────────
# fungsi untuk membaca tabel silver dari MinIO dalam format parquet
def read_silver(spark, path: str):
    return spark.read.parquet(path)


# ── AGGREGATE TRANSACTIONS ────────────────────────────────────────────────────
# fungsi untuk menghitung agregasi dari tabel transaksi per product_id
def aggregate_transactions(df_trx):
    step = "Aggregate transactions"
    log_start(step)
    try:
        # ambil tanggal terbaru dari data sebagai referensi "hari ini"
        df_trx = df_trx.withColumn(
            "transaction_date",
            F.col("transaction_date")
        )
        
        max_date_df = df_trx.select(F.max("transaction_date").alias("max_date"))
        df_trx = df_trx.crossJoin(max_date_df)
        
        df_velocity = (
             df_trx
            .filter(
                (F.upper(F.col("transaction_type")) == "SALE") &
                (F.col("transaction_date") >= F.expr("max_date - interval 30 days"))
            )
            .groupBy("product_id")
            .agg(F.sum(F.abs(F.col("quantity_change"))).alias("sales_velocity"))
        )

        # total_sales dan transaction_frequency dari semua transaksi SALE
        df_agg = (
            df_trx
            .filter(F.upper(F.col("transaction_type")) == "SALE")
            .groupBy("product_id")
            .agg(
                F.sum(F.abs(F.col("quantity_change"))).alias("total_sales"),
                F.count("*").alias("transaction_frequency"),
            )
        )

        # net_quantity_change: total perubahan stok dari semua tipe transaksi
        df_net = (
            df_trx
            .groupBy("product_id")
            .agg(F.sum("quantity_change").alias("net_quantity_change"))
        )

        # gabungkan semua agregasi berdasarkan product_id
        df_result = (
            df_agg
            .join(df_velocity, on="product_id", how="left")
            .join(df_net,      on="product_id", how="left")
        )

        log_success(step)
        return df_result
    except Exception as e:
        log_failed(step, e)

# ── JOIN ──────────────────────────────────────────────────────────────────────
# fungsi untuk menggabungkan tiga tabel silver menjadi satu dataframe
def join_tables(df_inv, df_trx_agg, df_sup):
    step = "Join silver tables"
    log_start(step)
    try:
        # rename kolom supplier agar tidak bentrok dengan kolom inventory saat join
        df_sup = (
            df_sup
            .withColumnRenamed("average_lead_time_days", "sup_lead_time")
            .withColumnRenamed("reliability_index",      "sup_reliability")
        )

        # left join: inventory ← agregasi transaksi ← supplier
        df = (
            df_inv
            .join(df_trx_agg, on="product_id", how="left")
            .join(df_sup,     on="supplier_id", how="left")
            .dropDuplicates(["product_id"])
        )

        log_success(step)
        return df
    except Exception as e:
        log_failed(step, e)

# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────
# fungsi untuk membangun semua fitur ML dari dataframe gabungan
def build_features(df):
    step = "Feature engineering"
    log_start(step)
    try:
        # tentukan kolom sumber; gunakan kolom supplier sebagai fallback, dengan nilai default jika 0
        stock_qty     = F.col("stock_quantity")        if "stock_quantity"        in df.columns else F.lit(0.0)
        
        # gunakan sup_lead_time, tapi jika nilainya 0, ganti dengan default 5 hari (reasonable supplier lead time)
        if "average_lead_time_days" in df.columns:
            avg_lead_time = F.col("average_lead_time_days")
        elif "sup_lead_time" in df.columns:
            avg_lead_time = F.when(F.col("sup_lead_time") == 0, F.lit(5.0)).otherwise(F.col("sup_lead_time"))
        else:
            avg_lead_time = F.lit(5.0)  # default 5 hari jika tidak ada kolom lead time
        
        reliability   = F.col("reliability_index")     if "reliability_index"     in df.columns else F.col("sup_reliability")

        # Feature 1: sales_velocity — sudah dihitung di aggregate_transactions, pastikan tipenya double
        df = df.withColumn("sales_velocity", F.col("sales_velocity").cast("double"))
        log_feature("sales_velocity", 1)

        # Feature 2: stock_on_hand = stock_quantity + net_quantity_change
        df = df.withColumn("stock_on_hand", (stock_qty + F.col("net_quantity_change")).cast("double"))
        log_feature("stock_on_hand", 2)

        # Feature 3: procurement_lead_time = (datediff(date_received, last_order_date) + avg_lead_time) / 2
        # kalau kolom tanggal tidak ada, anggap selisih = 0, tapi avg_lead_time sudah punya default
        if "date_received" in df.columns and "last_order_date" in df.columns:
            date_diff = F.datediff(F.to_date(F.col("date_received")), F.to_date(F.col("last_order_date"))).cast("double")
        else:
            date_diff = F.lit(0.0)

        df = df.withColumn(
            "procurement_lead_time",
            F.greatest(
                F.lit(1.0),
                ((date_diff + avg_lead_time) / F.lit(2.0)).cast("double")
            )
        )
        log_feature("procurement_lead_time", 3)

        # kolom turunan yang dibutuhkan oleh fitur 4–8
        df = df.withColumn("avg_daily_demand", (F.col("total_sales") / F.lit(30)).cast("double"))
        df = df.withColumn(
            "reorder_point",
            F.ceil(
                F.col("avg_daily_demand") * F.col("procurement_lead_time")
            )
        )

        # Feature 4: order_buffer_index = stock_on_hand / sales_velocity (hindari bagi nol)
        df = df.withColumn(
            "order_buffer_index",
            F.when(F.col("sales_velocity") == 0, F.lit(0.0))
             .otherwise((F.col("stock_on_hand") / F.col("sales_velocity")).cast("double"))
        )
        log_feature("order_buffer_index", 4)

        # Feature 5: supplier_risk = 1 - reliability_index, diclamp ke [0, 1]
        df = df.withColumn(
            "supplier_risk",
            F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.lit(1.0) - reliability).cast("double")))
        )
        log_feature("supplier_risk", 5)

        # Feature 6: demand_to_stock_ratio = avg_daily_demand / stock_on_hand (hindari bagi nol)
        df = df.withColumn(
            "demand_to_stock_ratio",
            F.when(F.col("stock_on_hand") == 0, F.lit(0.0))
             .otherwise((F.col("avg_daily_demand") / F.col("stock_on_hand")).cast("double"))
        )
        log_feature("demand_to_stock_ratio", 6)

        # Feature 7: log_sales = log(1 + total_sales)
        df = df.withColumn("log_sales", F.log1p(F.col("total_sales").cast("double")))
        log_feature("log_sales", 7)

        # Feature 8: adjusted_reorder_point = (avg_daily_demand * procurement_lead_time) * (1 + supplier_risk)
        df = df.withColumn(
            "adjusted_reorder_point",
            F.ceil(F.col("avg_daily_demand") * F.col("procurement_lead_time") * (F.lit(1.0) + F.col("supplier_risk"))).cast("double")
        )
        log_feature("adjusted_reorder_point", 8)

        # Target 1: stockout_risk = 1 jika stock_on_hand < reorder_point, selainnya 0
        df = df.withColumn(
            "stockout_risk",
            F.when(F.col("stock_on_hand") < F.col("reorder_point"), F.lit(1))
             .otherwise(F.lit(0))
             .cast("int")
        )

        log_success(step)
        return df
    except Exception as e:
        log_failed(step, e)

# ── SELECT FINAL COLUMNS ──────────────────────────────────────────────────────
# fungsi untuk memilih kolom final dan memastikan tipe data sudah benar
def select_ml_columns(df):
    step = "Select final ML columns"
    log_start(step)
    try:
        # daftar kolom wajib di output akhir
        final_cols = [
            "product_id",
            "sales_velocity",
            "stock_on_hand",
            "procurement_lead_time",
            "order_buffer_index",
            "supplier_risk",
            "demand_to_stock_ratio",
            "log_sales",
            "total_sales",
            "avg_daily_demand",
            "transaction_frequency",
            "reorder_point",
            "adjusted_reorder_point",
            "stockout_risk",
        ]

        # tambahkan kolom yang belum ada sebagai 0.0 agar select tidak gagal
        for col in final_cols:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(0.0))

        # pilih kolom, buang baris tanpa product_id, isi null, hilangkan duplikat
        df = (
            df.select(final_cols)
              .dropna(subset=["product_id"])
              .fillna(0)
              .dropDuplicates(["product_id"])
        )

        log_success(step)
        return df
    except Exception as e:
        log_failed(step, e)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    global _step_total
    _step_total = 8  # total langkah yang dilacak progress-nya

    # path silver (input)
    BUCKET     = "s3a://datalake-kelompok2"
    SILVER_TRX = f"{BUCKET}/silver/stock_transactions"
    SILVER_INV = f"{BUCKET}/silver/grocery_inventory"
    SILVER_SUP = f"{BUCKET}/silver/suppliers"

    # path gold (output)
    gold_features   = "s3a://datalake-kelompok2/gold/features/"
    gold_aggregates = "s3a://datalake-kelompok2/gold/aggregates/"
    gold_ml_ready   = "s3a://datalake-kelompok2/gold/ml_ready/"

    # membuat spark session
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # baca tabel silver dari MinIO
    df_trx = spark.read.parquet(SILVER_TRX)
    df_inv = spark.read.parquet(SILVER_INV)
    df_sup = spark.read.parquet(SILVER_SUP)

    print(f"  stock_transactions : {df_trx.count()} baris, kolom: {df_trx.columns}")
    print(f"  grocery_inventory  : {df_inv.count()} baris, kolom: {df_inv.columns}")
    print(f"  suppliers_info     : {df_sup.count()} baris, kolom: {df_sup.columns}")

    # hitung agregasi transaksi per product_id
    df_trx_agg = aggregate_transactions(df_trx)

    # gabungkan ketiga tabel
    df_joined = join_tables(df_inv, df_trx_agg, df_sup)

    # bangun semua fitur ML
    df_featured = build_features(df_joined)
    df_featured.cache()  # cache karena dipakai lebih dari sekali

    # pilih dan bersihkan kolom final
    df_ml = select_ml_columns(df_featured)
    df_ml.cache()

    df_featured.unpersist()
    df_ml.unpersist()

    # slice dataset fitur (tanpa kolom target)
    df_features_only = df_ml.drop("stockout_risk")

    # slice dataset agregasi
    df_aggregates = df_ml.select(
        "product_id", "total_sales", "avg_daily_demand",
        "transaction_frequency", "reorder_point", "stockout_risk"
    )

    # tulis semua output ke gold layer dalam format parquet
    # ── WRITE FEATURES ─────────────────────────────────────────────
    step = "Write features dataset"
    log_start(step)
    try:
        df_features_only.write.mode("overwrite").parquet(gold_features)
        log_success(step)
    except Exception as e:
        log_failed(step, e)


    # ── WRITE AGGREGATES ───────────────────────────────────────────
    step = "Write aggregates dataset"
    log_start(step)
    try:
        df_aggregates.write.mode("overwrite").parquet(gold_aggregates)
        log_success(step)
    except Exception as e:
        log_failed(step, e)


    # ── WRITE ML READY ─────────────────────────────────────────────
    step = "Write ML-ready dataset"
    log_start(step)
    try:
        df_ml.write.mode("overwrite").parquet(gold_ml_ready)
        log_success(step)
    except Exception as e:
        log_failed(step, e)

    # tampilkan jumlah baris hasil
    print("Gold counts:")
    print("features:",   df_features_only.count())
    print("aggregates:", df_aggregates.count())
    print("ml_ready:",   df_ml.count())

    # ringkasan akhir
    print("\n[FINISH] GOLD layer completed ✅✅✅")
    print("[ℹ️ INFO] Output:")
    print(f"  - {gold_features}")
    print(f"  - {gold_aggregates}")
    print(f"  - {gold_ml_ready}")

    spark.stop()

# entry point
# jika file ini dijalankan langsung, eksekusi fungsi main()
if __name__ == "__main__":
    main()