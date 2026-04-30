"""
GOLD LAYER - Feature Engineering & Target Variable Creation
=============================================================
Proyek BDA Kelompok 2: Prediksi Stockout & Optimalisasi Reorder Point
Memenuhi ketentuan PRD Section 4.1 (Teknik Analisis & Detail Implementasi Analisis)

Input  : MinIO silver/ (stock_transactions, grocery_inventory)
         MinIO raw/    (suppliers_info.json)
Output : MinIO gold/   (gold_features) dalam format Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
import traceback

# ─────────────────────────────────────────────
# 1. SPARK SESSION
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("GoldLayer-FeatureEngineering")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-kelompok2:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") # Tambahkan ini
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# 2. Pastikan fungsi normalisasi sama persis dengan Silver
def normalize_cols(df):
    import re
    for col in df.columns:
        # Mengikuti pola Silver: strip -> lower -> replace space/strip/slash
        new_name = col.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        if col != new_name:
            df = df.withColumnRenamed(col, new_name)
    return df

# ─────────────────────────────────────────────
# 2. BACA DATA SILVER + RAW SUPPLIERS
# ─────────────────────────────────────────────
print("\n[1/6] Membaca data Silver dan Raw Suppliers...")
BUCKET     = "s3a://datalake-kelompok2"
SILVER_TRX = f"{BUCKET}/silver/stock_transactions"
SILVER_INV = f"{BUCKET}/silver/grocery_inventory"
SILVER_SUP = f"{BUCKET}/silver/suppliers"
GOLD_OUT   = f"{BUCKET}/gold/gold_features"
df_trx = spark.read.parquet(SILVER_TRX)
df_inv = spark.read.parquet(SILVER_INV)
df_sup = spark.read.parquet(SILVER_SUP)

print(f"  stock_transactions : {df_trx.count()} baris, kolom: {df_trx.columns}")
print(f"  grocery_inventory  : {df_inv.count()} baris, kolom: {df_inv.columns}")
print(f"  suppliers_info     : {df_sup.count()} baris, kolom: {df_sup.columns}")

# ─────────────────────────────────────────────
# 3. NORMALISASI NAMA KOLOM (defensive)
# ─────────────────────────────────────────────
def normalize_cols(df):
    """Lowercase + strip + spasi/dash -> underscore (sama seperti silver layer)."""
    import re
    new_cols = []
    for c in df.columns:
        nc = c.strip().lower()
        nc = re.sub(r"[\s\-/]+", "_", nc)
        new_cols.append(nc)
    return df.toDF(*new_cols)

df_trx = normalize_cols(df_trx)
df_inv = normalize_cols(df_inv)
df_sup = normalize_cols(df_sup)

# ─────────────────────────────────────────────
# 4. FITUR 1 – SALES VELOCITY
# ─────────────────────────────────────────────
# PRD: ABS(SUM(quantity_change)) WHERE transaction_type = 'SALE'
# Representasi: total unit terjual per produk (dari seluruh histori transaksi)
# Dibagi jumlah hari aktif untuk mendapat rata-rata harian (daily_sales_velocity)
print("\n[2/6] Menghitung Sales Velocity...")

# Deteksi kolom quantity_change dan transaction_type
trx_cols = df_trx.columns

qty_col  = next((c for c in trx_cols if "quantity" in c), None)
type_col = next((c for c in trx_cols if "type" in c or "transaction_type" in c), None)
date_col = next((c for c in trx_cols if "date" in c or "time" in c), None)
prod_col = next((c for c in trx_cols if "product" in c or "product_id" in c), None)

print(f"  Kolom qty  : {qty_col}")
print(f"  Kolom type : {type_col}")
print(f"  Kolom date : {date_col}")
print(f"  Kolom prod : {prod_col}")

# Cast quantity ke double
df_trx = df_trx.withColumn(qty_col, F.col(qty_col).cast(DoubleType()))

# Filter transaksi SALE (case-insensitive)
df_sales = df_trx.filter(F.upper(F.col(type_col)).isin("SALE", "SALES", "OUT", "KELUAR"))

# Jika tidak ada baris SALE, ambil semua transaksi bertanda negatif (pengurangan stok)
sale_count = df_sales.count()
if sale_count == 0:
    print("  [WARN] Tidak ada transaksi bertipe SALE, fallback ke quantity_change < 0")
    df_sales = df_trx.filter(F.col(qty_col) < 0)

# Hitung total sales per produk
df_sales_agg = (
    df_sales
    .groupBy(prod_col)
    .agg(
        F.abs(F.sum(qty_col)).alias("total_sales"),
        F.countDistinct(F.to_date(F.col(date_col))).alias("active_days")
    )
)

# Daily sales velocity = total_sales / active_days (min 1 hari agar tidak div-by-zero)
df_sales_agg = df_sales_agg.withColumn(
    "daily_sales_velocity",
    F.when(F.col("active_days") > 0,
           F.col("total_sales") / F.col("active_days"))
    .otherwise(F.col("total_sales"))
)

print(f"  Sales velocity dihitung untuk {df_sales_agg.count()} produk")

# ─────────────────────────────────────────────
# 5. FITUR 2 – STOCK ON HAND
# ─────────────────────────────────────────────
# PRD: Stock_Quantity (Initial) + SUM(quantity_change) dari seluruh transaksi
print("\n[3/6] Menghitung Stock On Hand...")

inv_cols = df_inv.columns

inv_prod_col  = next((c for c in inv_cols if "product" in c or "product_id" in c), inv_cols[0])
inv_stock_col = next((c for c in inv_cols if "stock" in c and "quantity" in c
                      or c == "stock_quantity"), None)
if inv_stock_col is None:
    inv_stock_col = next((c for c in inv_cols if "quantity" in c), inv_cols[1])

print(f"  Kolom produk inventory : {inv_prod_col}")
print(f"  Kolom stok awal        : {inv_stock_col}")

# Cast stock quantity ke double
df_inv = df_inv.withColumn(inv_stock_col, F.col(inv_stock_col).cast(DoubleType()))

# SUM semua quantity_change per produk (semua jenis transaksi)
df_trx_total = (
    df_trx
    .groupBy(prod_col)
    .agg(F.sum(qty_col).alias("total_quantity_change"))
)

# Join inventory dengan total pergerakan stok
df_stock = (
    df_inv
    .join(df_trx_total,
          df_inv[inv_prod_col] == df_trx_total[prod_col],
          how="left")
    .withColumn(
        "stock_on_hand",
        F.col(inv_stock_col) + F.coalesce(F.col("total_quantity_change"), F.lit(0.0))
    )
    # Pastikan tidak negatif (floor 0)
    .withColumn("stock_on_hand", F.greatest(F.col("stock_on_hand"), F.lit(0.0)))
)

# ─────────────────────────────────────────────
# 6. FITUR 3 – PROCUREMENT LEAD TIME
# ─────────────────────────────────────────────
# PRD: (datediff(Date_Received, Last_Order_Date) + average_lead_time_days) / 2
print("\n[4/6] Menghitung Procurement Lead Time...")

date_received_col  = next((c for c in inv_cols if "date_received" in c or "received" in c), None)
last_order_col     = next((c for c in inv_cols if "last_order" in c or "order_date" in c), None)

sup_cols = df_sup.columns
sup_prod_col      = next((c for c in sup_cols if "product" in c), None)
avg_lead_time_col = next((c for c in sup_cols if "lead_time" in c or "leadtime" in c), None)
sup_id_col        = next((c for c in sup_cols if "supplier" in c and "id" in c), None)

print(f"  date_received_col  : {date_received_col}")
print(f"  last_order_col     : {last_order_col}")
print(f"  avg_lead_time_col  : {avg_lead_time_col}")

# Hitung datediff dari inventory
if date_received_col and last_order_col:
    df_stock = df_stock.withColumn(
        "actual_lead_time",
        F.datediff(
            F.to_date(F.col(date_received_col)),
            F.to_date(F.col(last_order_col))
        ).cast(DoubleType())
    )
else:
    print("  [WARN] Kolom date_received / last_order_date tidak ditemukan, actual_lead_time = NULL")
    df_stock = df_stock.withColumn("actual_lead_time", F.lit(None).cast(DoubleType()))

# Gabungkan dengan supplier (join by product_id jika ada, otherwise cross-join median)
if sup_prod_col and avg_lead_time_col:
    df_sup_clean = df_sup.select(
        F.col(sup_prod_col).alias("sup_product_id"),
        F.col(avg_lead_time_col).cast(DoubleType()).alias("avg_lead_time_days")
    )
    df_stock = df_stock.join(df_sup_clean,
                             df_stock[inv_prod_col] == df_sup_clean["sup_product_id"],
                             how="left")
elif avg_lead_time_col:
    # Tidak ada join key, ambil rata-rata global supplier
    median_lead = df_sup.agg(
        F.percentile_approx(F.col(avg_lead_time_col).cast(DoubleType()), 0.5)
        .alias("median_lt")
    ).collect()[0]["median_lt"]
    df_stock = df_stock.withColumn("avg_lead_time_days", F.lit(float(median_lead)))
else:
    print("  [WARN] avg_lead_time_days tidak ditemukan di suppliers, default = 7")
    df_stock = df_stock.withColumn("avg_lead_time_days", F.lit(7.0))

# PRD Formula: (actual_lead_time + avg_lead_time_days) / 2
df_stock = df_stock.withColumn(
    "procurement_lead_time",
    (
        F.coalesce(F.col("actual_lead_time"), F.col("avg_lead_time_days"))
        + F.coalesce(F.col("avg_lead_time_days"), F.lit(7.0))
    ) / 2.0
)

# ─────────────────────────────────────────────
# 7. GABUNGKAN SEMUA FITUR
# ─────────────────────────────────────────────
print("\n[5/6] Menggabungkan semua fitur ke Gold table...")

# Join sales velocity ke stock table
df_gold = (
    df_stock
    .join(df_sales_agg.select(
              F.col(prod_col).alias("sv_product_id"),
              "total_sales",
              "active_days",
              "daily_sales_velocity"
          ),
          df_stock[inv_prod_col] == F.col("sv_product_id"),
          how="left")
)

# Isi null daily_sales_velocity dengan 0
df_gold = df_gold.withColumn(
    "daily_sales_velocity",
    F.coalesce(F.col("daily_sales_velocity"), F.lit(0.0))
)

# ─────────────────────────────────────────────
# 8. FITUR 4 – ORDER BUFFER INDEX
# ─────────────────────────────────────────────
# PRD: Current_Stock_On_Hand / Daily_Sales_Velocity
# Menggambarkan berapa hari stok akan bertahan
print("  Menghitung Order Buffer Index...")

df_gold = df_gold.withColumn(
    "order_buffer_index",
    F.when(F.col("daily_sales_velocity") > 0,
           F.col("stock_on_hand") / F.col("daily_sales_velocity"))
    .otherwise(F.lit(None).cast(DoubleType()))  # undefined jika velocity = 0
)

# ─────────────────────────────────────────────
# 9. TARGET 1 – STOCKOUT RISK (Klasifikasi 0/1)
# ─────────────────────────────────────────────
# Logika: produk berisiko jika stok on hand <= safety stock
# Safety stock = daily_sales_velocity * procurement_lead_time
# stockout_risk = 1 jika stock_on_hand <= safety_stock
print("  Menghitung stockout_risk (target klasifikasi)...")

df_gold = df_gold.withColumn(
    "safety_stock",
    F.col("daily_sales_velocity") * F.col("procurement_lead_time")
)

df_gold = df_gold.withColumn(
    "stockout_risk",
    F.when(
        F.col("stock_on_hand") <= F.coalesce(F.col("safety_stock"), F.lit(0.0)),
        F.lit(1)
    ).otherwise(F.lit(0)).cast(IntegerType())
)

# ─────────────────────────────────────────────
# 10. TARGET 2 – REORDER POINT (Regresi)
# ─────────────────────────────────────────────
# Rumus standar ROP = (daily_sales_velocity * lead_time) + safety_stock
# safety_stock sudah dihitung di atas (= daily_sales_velocity * lead_time)
# ROP = 2 * safety_stock  (double coverage)
# Atau secara eksplisit:
# reorder_point = daily_sales_velocity * procurement_lead_time * 2
print("  Menghitung reorder_point (target regresi)...")

# Ambil reorder_level dari inventory sebagai referensi baseline
reorder_lvl_col = next((c for c in inv_cols if "reorder" in c and "level" in c), None)
if reorder_lvl_col:
    df_gold = df_gold.withColumn(
        reorder_lvl_col,
        F.col(reorder_lvl_col).cast(DoubleType())
    )
    # ROP final = rata-rata antara formula dan historical reorder_level
    df_gold = df_gold.withColumn(
        "reorder_point",
        (
            (F.col("daily_sales_velocity") * F.col("procurement_lead_time") * 2)
            + F.coalesce(F.col(reorder_lvl_col), F.lit(0.0))
        ) / 2.0
    )
else:
    # Jika tidak ada kolom reorder_level, pakai formula murni
    df_gold = df_gold.withColumn(
        "reorder_point",
        F.col("daily_sales_velocity") * F.col("procurement_lead_time") * 2
    )

# Pastikan reorder_point tidak negatif
df_gold = df_gold.withColumn(
    "reorder_point",
    F.greatest(F.col("reorder_point"), F.lit(0.0))
)

# ─────────────────────────────────────────────
# 11. PILIH KOLOM FINAL GOLD TABLE
# ─────────────────────────────────────────────
print("  Menyusun kolom final Gold table...")

# Kolom inti yang selalu ada
gold_base_cols = [
    F.col(inv_prod_col).alias("product_id"),
    F.col(inv_stock_col).alias("initial_stock_quantity"),
    F.col("stock_on_hand"),
    F.col("total_sales"),
    F.col("active_days"),
    F.col("daily_sales_velocity"),
    F.col("actual_lead_time"),
    F.col("avg_lead_time_days"),
    F.col("procurement_lead_time"),
    F.col("order_buffer_index"),
    F.col("safety_stock"),
    # Targets
    F.col("stockout_risk"),
    F.col("reorder_point"),
]

# Tambahkan reorder_level jika ada
if reorder_lvl_col:
    gold_base_cols.insert(-2, F.col(reorder_lvl_col).alias("historical_reorder_level"))

# Tambahkan kolom supplier product reference jika ada
if sup_prod_col and avg_lead_time_col:
    gold_base_cols.insert(1, F.col("sup_product_id").alias("supplier_product_ref"))

df_gold_final = df_gold.select(gold_base_cols)

# Drop duplikat berdasarkan product_id
df_gold_final = df_gold_final.dropDuplicates(["product_id"])

# ─────────────────────────────────────────────
# 12. VALIDASI & STATISTIK
# ─────────────────────────────────────────────
total      = df_gold_final.count()
stockout_1 = df_gold_final.filter(F.col("stockout_risk") == 1).count()
stockout_0 = df_gold_final.filter(F.col("stockout_risk") == 0).count()

print("\n" + "=" * 60)
print("VALIDASI GOLD LAYER")
print("=" * 60)
print(f"  Total produk di Gold table    : {total}")
print(f"  Produk BERISIKO stockout  (1) : {stockout_1}  ({100*stockout_1/max(total,1):.1f}%)")
print(f"  Produk AMAN dari stockout (0) : {stockout_0}  ({100*stockout_0/max(total,1):.1f}%)")
print()

print("  Sample statistik fitur:")
df_gold_final.select(
    "daily_sales_velocity",
    "stock_on_hand",
    "procurement_lead_time",
    "order_buffer_index",
    "safety_stock",
    "reorder_point"
).describe().show(truncate=False)

print("  Schema Gold table:")
df_gold_final.printSchema()

print("  Sample 5 baris Gold table:")
df_gold_final.show(5, truncate=False)

# ─────────────────────────────────────────────
# 13. SIMPAN KE MINIO GOLD/
# ─────────────────────────────────────────────
print(f"\n[6/6] Menyimpan Gold table ke {GOLD_OUT} ...")

(
    df_gold_final
    .coalesce(1)  # 1 file parquet agar mudah dibaca downstream
    .write
    .mode("overwrite")
    .parquet(GOLD_OUT)
)

print(f"  Gold layer berhasil disimpan ke: {GOLD_OUT}")
print("\n" + "=" * 60)
print("✅ GOLD LAYER SELESAI")
print("=" * 60)

spark.stop()