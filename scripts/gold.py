import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────
def make_spark(app_name: str = "gold-layer-processing"):
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    spark = (
        SparkSession.builder.appName(app_name)
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


# ─────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────
_step_done  = 0
_step_total = 0

def log_start(step):                        print(f"[START] {step}")
def log_feature(name, idx, total):         print(f"  [✅ FEATURE {idx}/{total}] {name}")
def log_success(step):
    global _step_done; _step_done += 1
    print(f"[✅ SUCCESS] {step} | Progress: {_step_done}/{_step_total}")
def log_failed(step, error):
    print(f"[❌ FAILED] {step}: {error}"); raise error


# ─────────────────────────────────────────────
# Preprocessing
# ─────────────────────────────────────────────
def normalize_colname(c):
    return c.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")

def preprocess(df):
    # Normalise column names
    for old in df.columns:
        new = normalize_colname(old)
        if old != new:
            df = df.withColumnRenamed(old, new)
    # Trim string columns
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    # Cast obvious date/timestamp columns
    for name, dtype in df.dtypes:
        if any(k in name.lower() for k in ("date", "time", "created")):
            if dtype == "string":
                df = df.withColumn(name, F.to_timestamp(F.col(name)))
    return df.dropDuplicates()


# ─────────────────────────────────────────────
# Aggregate stock_transactions
# ─────────────────────────────────────────────
def aggregate_transactions(df_trx):
    """
    Dari stock_transactions kita tarik:
      - sales_velocity        : total unit SALE dalam 30 hari terakhir
      - total_sales           : total unit SALE sepanjang waktu
      - transaction_frequency : jumlah transaksi SALE
    """
    step = "Aggregate transactions"; log_start(step)
    try:
        # Tanggal terbaru dalam dataset (bukan hari ini) agar deterministik
        max_date_df = df_trx.select(F.max("transaction_date").alias("max_date"))
        df_trx = df_trx.crossJoin(max_date_df)

        # Sales velocity — 30 hari terakhir
        df_velocity = (
            df_trx
            .filter(
                (F.upper(F.col("transaction_type")) == "SALE") &
                (F.col("transaction_date") >= F.expr("max_date - interval 30 days"))
            )
            .groupBy("product_id")
            .agg(F.sum(F.abs(F.col("quantity_change"))).alias("sales_velocity"))
        )

        # Total sales & frekuensi — all-time (dipakai sebagai fitur informatif)
        df_agg = (
            df_trx
            .filter(F.upper(F.col("transaction_type")) == "SALE")
            .groupBy("product_id")
            .agg(
                F.sum(F.abs(F.col("quantity_change"))).alias("total_sales"),
                F.count("*").alias("transaction_frequency"),
            )
        )

        result = df_agg.join(df_velocity, on="product_id", how="left")
        log_success(step)
        return result
    except Exception as e:
        log_failed(step, e)


# ─────────────────────────────────────────────
# Join silver tables
# ─────────────────────────────────────────────
def join_tables(df_inv, df_trx_agg, df_sup):
    """
    Gabungkan:
      Grocery_Inventory  ← kiri (product_id)
      stock_transactions ← aggregasi transaksi
      suppliers_info     ← supplier dimensi
    """
    step = "Join silver tables"; log_start(step)
    try:
        df_sup = (
            df_sup
            .withColumnRenamed("average_lead_time_days", "sup_lead_time")
            .withColumnRenamed("reliability_index",      "sup_reliability")
        )

        df = (
            df_inv
            .join(df_trx_agg, on="product_id", how="left")
            .join(df_sup,     on="supplier_id", how="left")
            .dropDuplicates(["product_id"])
        )

        # Konfirmasi kolom kritis dari Grocery_Inventory ada
        for c in ["reorder_level", "reorder_quantity", "stock_quantity",
                  "date_received", "last_order_date", "sales_volume",
                  "inventory_turnover_rate"]:
            status = "✅ ditemukan" if c in df.columns else "⚠️  TIDAK ditemukan"
            print(f"  [{status}] kolom '{c}'")

        log_success(step)
        return df
    except Exception as e:
        log_failed(step, e)


# ─────────────────────────────────────────────
# Feature Engineering
# ─────────────────────────────────────────────
def build_features(df):
    """
    Semua fitur dibangun dari kolom yang ada di kamus data:

    Grocery_Inventory  : stock_quantity, date_received, last_order_date,
                         sales_volume, inventory_turnover_rate, reorder_level,
                         reorder_quantity
    stock_transactions : sales_velocity (30d), total_sales, transaction_frequency
    suppliers_info     : sup_lead_time, sup_reliability
    """
    step = "Feature engineering"; log_start(step)
    TOTAL = 10
    try:
        # ── Kolom sumber ──────────────────────────────────────────────────────
        stock_qty   = F.col("stock_quantity").cast("double")          # stok fisik saat ini
        sup_lead    = F.when(F.col("sup_lead_time") == 0, F.lit(5.0)) \
                       .otherwise(F.col("sup_lead_time").cast("double"))
        reliability = F.col("sup_reliability").cast("double")         # 0–1

        # ── Feature 1: sales_velocity (unit/30 hari) ─────────────────────────
        # Sudah dari aggregate_transactions; cast agar konsisten
        df = df.withColumn("sales_velocity",
                F.coalesce(F.col("sales_velocity"), F.lit(0.0)).cast("double"))
        log_feature("sales_velocity — unit terjual 30 hari terakhir", 1, TOTAL)

        # ── Feature 2: stock_on_hand ──────────────────────────────────────────
        # PERBAIKAN: langsung dari stock_quantity (stok fisik di gudang)
        # Tidak ditambah net transaksi karena stock_quantity adalah kondisi terkini
        df = df.withColumn("stock_on_hand", stock_qty)
        log_feature("stock_on_hand — stok fisik terkini dari stock_quantity", 2, TOTAL)

        # ── Feature 3: avg_daily_demand ───────────────────────────────────────
        # PERBAIKAN: pakai sales_velocity (30d) bukan total_sales
        # agar mencerminkan demand aktual terkini, bukan rata-rata historis panjang
        df = df.withColumn("avg_daily_demand",
                (F.col("sales_velocity") / F.lit(30.0)).cast("double"))
        log_feature("avg_daily_demand — demand harian (sales_velocity/30)", 3, TOTAL)

        # ── Feature 4: procurement_lead_time ─────────────────────────────────
        # Rata-rata antara lead time aktual (date_received - last_order_date)
        # dan lead time dari master supplier
        date_diff = F.datediff(
            F.to_date(F.col("date_received")),
            F.to_date(F.col("last_order_date"))
        ).cast("double")

        df = df.withColumn("procurement_lead_time",
                F.greatest(
                    F.lit(1.0),
                    ((F.coalesce(date_diff, sup_lead) + sup_lead) / F.lit(2.0)).cast("double")
                ))
        log_feature("procurement_lead_time — rata-rata lead time aktual & supplier", 4, TOTAL)

        # ── Feature 5: supplier_risk ──────────────────────────────────────────
        # 1 - reliability_index  (skala 0–1, makin tinggi makin berisiko)
        df = df.withColumn("supplier_risk",
                F.greatest(F.lit(0.0),
                    F.least(F.lit(1.0),
                        (F.lit(1.0) - reliability).cast("double"))))
        log_feature("supplier_risk — risiko keterlambatan supplier (1 - reliability)", 5, TOTAL)

        # ── Feature 6: order_buffer_index ────────────────────────────────────
        # Berapa hari stok cukup vs kecepatan jual (30d)
        # Clip ke [0, 365] agar tidak ada outlier ekstrem
        df = df.withColumn("order_buffer_index",
                F.least(F.lit(365.0),
                    F.when(F.col("sales_velocity") == 0, F.lit(0.0))
                     .otherwise(
                         (F.col("stock_on_hand") / F.col("sales_velocity")).cast("double")
                     )))
        log_feature("order_buffer_index — stok/sales_velocity, clip 365 hari", 6, TOTAL)

        # ── Feature 7: stock_cover ────────────────────────────────────────────
        # Berapa hari stok bertahan berdasarkan avg_daily_demand
        df = df.withColumn("stock_cover",
                F.when(F.col("avg_daily_demand") == 0, F.lit(0.0))
                 .otherwise(
                     (F.col("stock_on_hand") / F.col("avg_daily_demand")).cast("double")
                 ))
        log_feature("stock_cover — estimasi hari stok bertahan", 7, TOTAL)

        # ── Feature 8: inventory_turnover_rate ───────────────────────────────
        # Langsung dari Grocery_Inventory (sudah dihitung di sumber)
        df = df.withColumn("inventory_turnover_rate",
                F.coalesce(F.col("inventory_turnover_rate"), F.lit(0.0)).cast("double"))
        log_feature("inventory_turnover_rate — dari dataset asli Grocery_Inventory", 8, TOTAL)

        # ── Feature 9: log_sales ──────────────────────────────────────────────
        # Log transform total_sales untuk mengurangi skewness
        df = df.withColumn("log_sales",
                F.log1p(F.coalesce(F.col("total_sales"), F.lit(0.0)).cast("double")))
        log_feature("log_sales — log1p(total_sales) untuk kurangi skewness", 9, TOTAL)

        # ── Feature 10: demand_to_stock_ratio ────────────────────────────────
        df = df.withColumn("demand_to_stock_ratio",
                F.when(F.col("stock_on_hand") == 0, F.lit(0.0))
                 .otherwise(
                     (F.col("avg_daily_demand") / F.col("stock_on_hand")).cast("double")
                 ))
        log_feature("demand_to_stock_ratio — avg_daily_demand / stock_on_hand", 10, TOTAL)

        # ── reorder_level: langsung dari dataset (TARGET Regresi bukan) ───────
        # Dipakai sebagai FITUR referensi / info saja
        df = df.withColumn("reorder_level",
                F.coalesce(F.col("reorder_level"), F.lit(0.0)).cast("double"))

        # ── reorder_quantity: langsung dari dataset (info EOQ asli) ──────────
        df = df.withColumn("reorder_quantity",
                F.coalesce(F.col("reorder_quantity"), F.lit(0.0)).cast("double"))

        # ── TARGET Regresi: reorder_point (ROP optimal kalkulasi) ─────────────
        # Formula: ceil(avg_daily_demand × procurement_lead_time × (1 + supplier_risk))
        # Ini ROP yang mempertimbangkan demand terkini + lead time + buffer risiko supplier
        df = df.withColumn("reorder_point",
                F.ceil(
                    F.col("avg_daily_demand")
                    * F.col("procurement_lead_time")
                    * (F.lit(1.0) + F.col("supplier_risk"))
                ).cast("double"))
        print(f"  [✅ TARGET REGRESI] reorder_point = ceil(avg_daily_demand × lead_time × (1 + supplier_risk))")

        # ── TARGET Klasifikasi: stockout_risk ─────────────────────────────────
        # 1 = stock_on_hand < reorder_point  → perlu segera order
        # 0 = stok masih aman
        df = df.withColumn("stockout_risk",
                F.when(F.col("stock_on_hand") < F.col("reorder_point"), F.lit(1))
                 .otherwise(F.lit(0))
                 .cast("int"))
        print(f"  [✅ TARGET KLASIFIKASI] stockout_risk = 1 jika stock_on_hand < reorder_point")

        log_success(step)
        return df
    except Exception as e:
        log_failed(step, e)


# ─────────────────────────────────────────────
# Pilih kolom final untuk ML
# ─────────────────────────────────────────────
def select_ml_columns(df):
    """
    Kolom X (fitur):
        sales_velocity, stock_on_hand, avg_daily_demand, procurement_lead_time,
        supplier_risk, order_buffer_index, stock_cover,
        inventory_turnover_rate, log_sales, demand_to_stock_ratio,
        total_sales, transaction_frequency,
        reorder_level, reorder_quantity   ← info dari dataset asli

    Target Y:
        reorder_point  ← RF Regressor  (ROP optimal kalkulasi)
        stockout_risk  ← RF Classifier (0 = aman, 1 = berisiko)
    """
    step = "Select final ML columns"; log_start(step)
    try:
        final_cols = [
            "product_id",
            # ── Fitur (X) ──────────────────────────────
            "sales_velocity",
            "stock_on_hand",
            "avg_daily_demand",
            "procurement_lead_time",
            "supplier_risk",
            "order_buffer_index",
            "stock_cover",
            "inventory_turnover_rate",
            "log_sales",
            "demand_to_stock_ratio",
            "total_sales",
            "transaction_frequency",
            # ── Referensi dari dataset asli ────────────
            "reorder_level",       # ROP asli di dataset (bukan target, hanya referensi)
            "reorder_quantity",    # EOQ asli
            # ── Target (Y) ─────────────────────────────
            "reorder_point",       # ← TARGET RF Regressor
            "stockout_risk",       # ← TARGET RF Classifier
        ]

        for col in final_cols:
            if col not in df.columns:
                print(f"  [⚠️  WARNING] '{col}' tidak ada di dataframe, diisi 0.0")
                df = df.withColumn(col, F.lit(0.0))

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


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    global _step_total; _step_total = 8

    BUCKET     = "s3a://datalake-kelompok2"
    SILVER_TRX = f"{BUCKET}/silver/stock_transactions"
    SILVER_INV = f"{BUCKET}/silver/Grocery_Inventory"
    SILVER_SUP = f"{BUCKET}/silver/suppliers"

    gold_features   = f"{BUCKET}/gold/features/"
    gold_aggregates = f"{BUCKET}/gold/aggregates/"
    gold_ml_ready   = f"{BUCKET}/gold/ml_ready/"

    # ── Init ─────────────────────────────────────────────────────────────────
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── Load silver ──────────────────────────────────────────────────────────
    df_trx = preprocess(spark.read.parquet(SILVER_TRX))
    df_inv = preprocess(spark.read.parquet(SILVER_INV))
    df_sup = preprocess(spark.read.parquet(SILVER_SUP))

    print(f"\n[INFO] Data loaded:")
    print(f"  stock_transactions : {df_trx.count()} baris | cols: {df_trx.columns}")
    print(f"  grocery_inventory  : {df_inv.count()} baris | cols: {df_inv.columns}")
    print(f"  suppliers_info     : {df_sup.count()} baris | cols: {df_sup.columns}\n")

    # ── Pipeline ─────────────────────────────────────────────────────────────
    df_trx_agg  = aggregate_transactions(df_trx)
    df_joined   = join_tables(df_inv, df_trx_agg, df_sup)
    df_featured = build_features(df_joined)
    df_featured.cache()

    df_ml = select_ml_columns(df_featured)
    df_ml.cache()
    df_featured.unpersist()

    # ── Pecah output ─────────────────────────────────────────────────────────
    # features  : semua fitur X (tanpa target)
    df_features_only = df_ml.drop("stockout_risk", "reorder_point")

    # aggregates: ringkasan bisnis per produk
    df_aggregates = df_ml.select(
        "product_id",
        "total_sales",
        "avg_daily_demand",
        "transaction_frequency",
        "reorder_level",           # ROP asli dari dataset
        "reorder_quantity",        # EOQ asli dari dataset
        "reorder_point",           # ROP optimal (output Regressor)
        "stockout_risk",           # label risiko (output Classifier)
    )

    # ── Write gold ───────────────────────────────────────────────────────────
    for (step_name, df_out, path) in [
        ("Write features dataset",  df_features_only, gold_features),
        ("Write aggregates dataset", df_aggregates,   gold_aggregates),
        ("Write ML-ready dataset",  df_ml,            gold_ml_ready),
    ]:
        log_start(step_name)
        try:
            df_out.write.mode("overwrite").parquet(path)
            log_success(step_name)
        except Exception as e:
            log_failed(step_name, e)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n[INFO] Gold row counts:")
    print(f"  features   : {df_features_only.count()} baris")
    print(f"  aggregates : {df_aggregates.count()} baris")
    print(f"  ml_ready   : {df_ml.count()} baris")

    df_ml.unpersist()

    print("\n[FINISH] GOLD layer completed ✅✅✅")
    print("  ┌──────────────────────────────────────────────────────────────┐")
    print("  │  TARGET MODEL                                                │")
    print("  │  reorder_point  → RF Regressor  (ROP optimal kalkulasi)     │")
    print("  │  stockout_risk  → RF Classifier (0 = aman, 1 = berisiko)    │")
    print("  │                                                              │")
    print("  │  REFERENSI DATASET ASLI                                      │")
    print("  │  reorder_level    → ROP original dari Grocery_Inventory     │")
    print("  │  reorder_quantity → EOQ original dari Grocery_Inventory     │")
    print("  └──────────────────────────────────────────────────────────────┘")

    spark.stop()


if __name__ == "__main__":
    main()