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
        for c in ["reorder_point_old", "reorder_quantity", "stock_quantity",
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
                         sales_volume, inventory_turnover_rate, reorder_point_old,
                         reorder_quantity
    stock_transactions : sales_velocity (30d), total_sales, transaction_frequency
    suppliers_info     : sup_lead_time, sup_reliability
    """
    step = "Feature engineering"; log_start(step)
    TOTAL = 5
    try:
        # ── Kolom sumber ──────────────────────────────────────────────────────
        stock_qty   = F.col("stock_quantity").cast("double")          # stok fisik saat ini
        sup_lead    = F.when(F.col("sup_lead_time") == 0, F.lit(5.0)) \
                       .otherwise(F.col("sup_lead_time").cast("double"))
        reliability = F.col("sup_reliability").cast("double")         # 0–1

        # ── Feature 1: stock_on_hand ──────────────────────────────────────────
        # PERBAIKAN: langsung dari stock_quantity (stok fisik di gudang)
        # Tidak ditambah net transaksi karena stock_quantity adalah kondisi terkini
        df = df.withColumn("stock_on_hand", stock_qty)
        log_feature("stock_on_hand — stok fisik terkini dari stock_quantity", 1, TOTAL)

        # ── Feature 2: avg_daily_demand ───────────────────────────────────────
        # Priority: total_sales (all-time) → sales_volume → default
        demand_from_total = F.when(F.col("total_sales").isNotNull() & (F.col("total_sales") > 0),
                                   F.col("total_sales") / F.lit(365.0))
        demand_from_sales_vol = F.coalesce(F.col("sales_volume"), F.lit(0.0))
        
        df = df.withColumn("avg_daily_demand",
                F.when(demand_from_total.isNotNull() & (demand_from_total > 0), 
                       demand_from_total)
                 .when(demand_from_sales_vol > 0,
                       demand_from_sales_vol / F.lit(365.0))
                 .otherwise(F.lit(1.0))  # minimum default untuk hindari div-by-zero
                 .cast("double"))
        log_feature("avg_daily_demand — total_sales→sales_volume (fallback: 1.0)", 2, TOTAL)

        # ── Feature 3: procurement_lead_time ─────────────────────────────────
        # Priority: lead time aktual → supplier lead time → default 5 hari
        date_diff = F.when(
            F.col("date_received").isNotNull() & F.col("last_order_date").isNotNull(),
            F.datediff(F.to_date(F.col("date_received")), F.to_date(F.col("last_order_date")))
                .cast("double")
        )

        df = df.withColumn("procurement_lead_time",
                F.greatest(F.lit(1.0),
                    F.coalesce(
                        F.when(date_diff.isNotNull() & (date_diff > 0), date_diff),
                        sup_lead
                    )
                ).cast("double"))
        log_feature("procurement_lead_time — actual→supplier (default: ≥1)", 3, TOTAL)

        # ── Feature 4: supplier_risk ──────────────────────────────────────────
        # 1 - reliability_index  (skala 0–1, makin tinggi makin berisiko)
        # Default 0.2 jika reliability tidak ada untuk variasi
        df = df.withColumn("supplier_risk",
                F.greatest(F.lit(0.0),
                    F.least(F.lit(1.0),
                        (F.lit(1.0) - F.coalesce(reliability, F.lit(0.8))).cast("double"))))
        log_feature("supplier_risk — 1 - reliability (default 0.2 jika null)", 4, TOTAL)

        # ── Feature 5: inventory_turnover_rate ───────────────────────────────
        # Langsung dari Grocery_Inventory (sudah dihitung di sumber)
        df = df.withColumn("inventory_turnover_rate",
                F.coalesce(F.col("inventory_turnover_rate"), F.lit(0.0)).cast("double"))
        log_feature("inventory_turnover_rate — dari dataset asli Grocery_Inventory", 5, TOTAL)

        # ── reorder_point_old: langsung dari dataset (TARGET Regresi bukan) ───────
        # Dipakai sebagai FITUR referensi / info saja
        df = df.withColumn("reorder_point_old",
                F.coalesce(F.col("reorder_point_old"), F.lit(0.0)).cast("double"))

        # ── reorder_quantity: langsung dari dataset (info EOQ asli) ──────────
        df = df.withColumn("reorder_quantity",
                F.coalesce(F.col("reorder_quantity"), F.lit(0.0)).cast("double"))

        # ── TARGET Regresi: reorder_point_new (ROP optimal kalkulasi) ─────────────
        # Formula: max(reorder_point_old, ceil(demand × lead_time × (1 + risk_buffer)))
        # Pastikan hasil > 0 dengan menggunakan reorder_point_old sebagai minimum
        rop_calc = (
            F.col("avg_daily_demand")
            * F.col("procurement_lead_time")
            * (F.lit(1.0) + F.col("supplier_risk") * F.lit(0.5))
        )
        df = df.withColumn("reorder_point_new",
                F.greatest(
                    F.coalesce(F.col("reorder_point_old"), F.lit(1.0)),
                    F.ceil(rop_calc).cast("double")
                ))
        print(f"  [✅ TARGET REGRESI] reorder_point_new = max(reorder_point_old, ceil(demand × lead_time × (1 + 0.5×risk)))")

        # ── TARGET Klasifikasi: stockout_risk ─────────────────────────────────
        # 1 = stock_on_hand < reorder_point_new  → perlu segera order
        # 0 = stok masih aman
        df = df.withColumn("stockout_risk",
                F.when(F.col("stock_on_hand") < F.col("reorder_point_new"), F.lit(1))
                 .otherwise(F.lit(0))
                 .cast("int"))
        print(f"  [✅ TARGET KLASIFIKASI] stockout_risk = 1 jika stock_on_hand < reorder_point_new")

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
        stock_on_hand, avg_daily_demand, procurement_lead_time,
        supplier_risk, inventory_turnover_rate,
        total_sales, transaction_frequency,
        reorder_point_old, reorder_quantity   ← info dari dataset asli

    Target Y:
        reorder_point_new  ← RF Regressor  (ROP optimal kalkulasi)
        stockout_risk      ← RF Classifier (0 = aman, 1 = berisiko)
    """
    step = "Select final ML columns"; log_start(step)
    try:
        final_cols = [
            "product_id",
            # ── Fitur (X) ──────────────────────────────
            "stock_on_hand",
            "avg_daily_demand",
            "procurement_lead_time",
            "supplier_risk",
            "inventory_turnover_rate",
            "total_sales",
            "transaction_frequency",
            # ── Referensi dari dataset asli ────────────
            "reorder_point_old",       # ROP asli di dataset (bukan target, hanya referensi)
            "reorder_quantity",        # EOQ asli
            # ── Target (Y) ─────────────────────────────
            "reorder_point_new",   # ← TARGET RF Regressor
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
    # gold_ml_ready   = f"{BUCKET}/gold/ml_ready/"
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
    df_features_only = df_ml.drop("stockout_risk", "reorder_point_new")

    # aggregates: ringkasan bisnis per produk
    df_aggregates = df_ml.select(
        "product_id",
        "total_sales",
        "avg_daily_demand",
        "transaction_frequency",
        "reorder_point_old",       # ROP asli dari dataset
        "reorder_quantity",        # EOQ asli dari dataset
        "reorder_point_new",       # ROP optimal (output Regressor)
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

    print("\n[✨ FINISH] GOLD layer completed ✅✅✅")
    print("  ┌──────────────────────────────────────────────────────────────┐")
    print("  │  TARGET MODEL                                                │")
    print("  │  reorder_point_new  → RF Regressor  (ROP optimal kalkulasi)  │")
    print("  │  stockout_risk      → RF Classifier (0 = aman, 1 = berisiko) │")
    print("  │                                                              │")
    print("  │  REFERENSI DATASET ASLI                                      │")
    print("  │  reorder_point_old  → ROP original dari Grocery_Inventory    │")
    print("  │  reorder_quantity   → EOQ original dari Grocery_Inventory    │")
    print("  └──────────────────────────────────────────────────────────────┘")

    spark.stop()


if __name__ == "__main__":
    main()