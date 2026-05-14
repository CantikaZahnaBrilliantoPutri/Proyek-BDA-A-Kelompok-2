"""
P03-A | Inventory Analytics — Modeling Script (OPTIMIZED)
==========================================================
Model 1 : Random Forest Regressor  → prediksi reorder_point_new optimal
Model 2 : Gradient Boosting Classifier → prediksi stockout_risk

INPUT  : s3a://datalake-kelompok2/gold/ml_ready/
OUTPUT : s3a://datalake-kelompok2/gold/modeling_results/

FITUR (5):
  1. stock_on_hand           — Stok fisik terkini
  2. avg_daily_demand        — Rata-rata permintaan harian
  3. procurement_lead_time   — Lead time pengadaan
  4. supplier_risk           — Risiko supplier
  5. inventory_turnover_rate — Tingkat perputaran inventory

TARGET:
  • reorder_point_new (Regresi)  — ROP optimal
  • stockout_risk (Klasifikasi)  — Risiko stockout (0=aman, 1=berisiko)

OPTIMIZATIONS:
  ✓ Feature Scaling (StandardScaler) — normalisasi input
  ✓ RF Regressor + GBT Classifier — model yang robust
  ✓ Hyperparameter tuning — reduce overfitting
  ✓ Subsampling — improve generalization
"""

import os
import logging

logging.basicConfig(level=logging.ERROR)

import matplotlib
matplotlib.use("Agg") 
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)

from pyspark.ml.classification import RandomForestClassifier


# ═══════════════════════════════════════════════════════════════════════════════
# 0. CONSTANTS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
BUCKET          = "s3a://datalake-kelompok2"
INPUT_PATH      = f"{BUCKET}/gold/ml_ready_capped/"
OUTPUT_PATH     = f"{BUCKET}/gold/modeling_results/"
# direktori lokal untuk simpan gambar
PLOT_DIR        = "plots"          

# ── 5 Fitur Terbaik (dari Gold Layer) ───────────────────────────────────────
FEATURE_COLS = [
    "stock_on_hand",                # Stok fisik terkini
    "avg_daily_demand",             # Rata-rata permintaan harian
    "procurement_lead_time",        # Lead time pengadaan
    "supplier_risk",                # Risiko supplier (1 - reliability_index)
    "inventory_turnover_rate",      # Tingkat perputaran inventory
]

TARGET_REG  = "reorder_point_new"
TARGET_CLF  = "stockout_risk"
REF_COL     = "reorder_point_old" # ROP lama dari dataset asli

# ── Optimized RF Parameters (reduce overfitting) ────────────────────────────
RF_PARAMS_REG = dict(
    numTrees=150,
    maxDepth=6,              # Reduced from 10 (less overfitting)
    minInstancesPerNode=5,   # Regularization
    subsamplingRate=0.8,     # Bootstrap sampling
    featureSubsetStrategy="sqrt",
    seed=42
)

RF_PARAMS_CLF = dict(
    numTrees=150,
    maxDepth=6,
    minInstancesPerNode=5,
    subsamplingRate=0.8,
    featureSubsetStrategy="sqrt",
    seed=42
)

# ── GBT Parameters (alternative model) ──────────────────────────────────────
GBT_PARAMS_REG = dict(
    maxIter=100,
    maxDepth=4,
    stepSize=0.1,           # Changed from learning_rate to stepSize
    subsamplingRate=0.8,
    seed=42
)

GBT_PARAMS_CLF = dict(
    maxIter=100,
    maxDepth=4,
    stepSize=0.1,           # Changed from learning_rate to stepSize
    subsamplingRate=0.8,
    seed=42
)

os.makedirs(PLOT_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. LOGGING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
_STEP_DONE  = 0
_STEP_TOTAL = 12

def log_start(step: str):
    print(f"\n[START] {step}")

def log_success(step: str):
    global _STEP_DONE
    _STEP_DONE += 1
    print(f"[✅ SUCCESS] {step}  ({_STEP_DONE}/{_STEP_TOTAL})")

def log_info(msg: str):
    print(f"[ℹ️ INFO] {msg}")

def log_warn(msg: str):
    print(f"[⚠️ WARN] {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SPARK SESSION
# ═══════════════════════════════════════════════════════════════════════════════
def make_spark() -> SparkSession:
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "minio-kelompok2:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    spark = (
        SparkSession.builder
        .appName("P03-A Modeling")
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
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DATA PREPARATION
# ═══════════════════════════════════════════════════════════════════════════════
def load_and_prepare(spark: SparkSession):
    step = "Data Preparation"
    log_start(step)

    df = spark.read.parquet(INPUT_PATH)
    log_info(f"Total rows loaded : {df.count()}")
    log_info(f"Columns available : {df.columns}")

    # Pastikan fitur yang diminta tersedia; fallback ke 0 bila tidak ada
    for col in FEATURE_COLS + [TARGET_REG, TARGET_CLF, REF_COL, "product_id"]:
        if col not in df.columns:
            log_warn(f"Kolom '{col}' tidak ditemukan → diisi 0.0")
            df = df.withColumn(col, F.lit(0.0))

    # Cast semua fitur ke double; handle null
    for col in FEATURE_COLS + [TARGET_REG, REF_COL]:
        df = df.withColumn(col, F.col(col).cast("double"))

    df = df.withColumn(TARGET_CLF, F.col(TARGET_CLF).cast("int"))
    
    # Isi semua null values dengan 0 (setelah cast)
    df = df.fillna({
        col: 0.0 for col in FEATURE_COLS + [TARGET_REG, REF_COL]
    })
    df = df.fillna({TARGET_CLF: 0})
    
    # Drop rows dengan product_id null (tidak bisa diprediksi tanpa ID)
    df = df.filter(F.col("product_id").isNotNull())
    
    log_info(f"Rows after null filtering : {df.count()}")

    # VectorAssembler: gabungkan fitur jadi satu kolom "features_raw"
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    df = assembler.transform(df)
    
    # StandardScaler: normalisasi fitur (penting untuk model performa)
    # withMean=False karena tree-based models tidak memerlukan centering
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)
    df = scaler.fit(df).transform(df)
    
    # Hapus kolom intermediate
    df = df.drop("features_raw")
    
    # Validasi: pastikan tidak ada NaN atau infinite values dalam features
    from pyspark.ml.functions import vector_to_array
    df_check = df.select(
        F.when(
            F.col("features").isNull() | 
            F.array_contains(vector_to_array(F.col("features")), float('nan')) |
            F.array_contains(vector_to_array(F.col("features")), float('inf')),
            1
        ).otherwise(0).alias("has_invalid")
    )
    invalid_count = df_check.filter(F.col("has_invalid") == 1).count()
    if invalid_count > 0:
        log_warn(f"Ditemukan {invalid_count} rows dengan invalid features (NaN/Inf)")
        df = df.filter(F.col("features").isNotNull())

    log_info(f"Schema preview:")
    df.printSchema()
    log_info(f"Final rows ready for training : {df.count()}")

    log_success(step)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TRAIN-TEST SPLIT
# ═══════════════════════════════════════════════════════════════════════════════
def split_data(df):
    step = "Train-Test Split (80/20, seed=42)"
    log_start(step)
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    test.cache()
    log_info(f"Train rows : {train.count()}")
    log_info(f"Test rows  : {test.count()}")
    log_success(step)
    return train, test


# ═══════════════════════════════════════════════════════════════════════════════
# 5. REGRESSION MODEL — Random Forest Regressor
# ═══════════════════════════════════════════════════════════════════════════════
def train_regressor(train, test):
    step = "Regression Model — Random Forest Regressor (optimized)"
    log_start(step)

    # Gunakan RF Regressor dengan parameter optimized (GBT kadang underperform untuk data kecil)
    rf_reg = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_REG,
        predictionCol="optimal_rop_pred",
        **RF_PARAMS_REG,
    )

    pipeline_reg = Pipeline(stages=[rf_reg])
    model_reg    = pipeline_reg.fit(train)
    pred_reg     = model_reg.transform(test)

    # Evaluasi
    def eval_reg(metric):
        return RegressionEvaluator(
            labelCol=TARGET_REG,
            predictionCol="optimal_rop_pred",
            metricName=metric,
        ).evaluate(pred_reg)

    mae  = eval_reg("mae")
    rmse = eval_reg("rmse")
    r2   = eval_reg("r2")
    
    # Hitung Mean Absolute Percentage Error (MAPE) untuk context
    from pyspark.sql.functions import abs as spark_abs, mean as spark_mean, lit
    mape_df = pred_reg.withColumn(
        "ape",
        spark_abs((F.col(TARGET_REG) - F.col("optimal_rop_pred")) / F.col(TARGET_REG)) * 100
    )
    mape = mape_df.select(spark_mean("ape")).collect()[0][0]

    print("\n  ┌─────────────────────────────────┐")
    print("  │  REGRESSION METRICS             │")
    print(f"  │  MAE  : {mae:>10.4f}              │")
    print(f"  │  RMSE : {rmse:>10.4f}              │")
    print(f"  │  MAPE : {mape:>10.2f}%             │")
    print(f"  │  R²   : {r2:>10.4f}              │")
    print("  └─────────────────────────────────┘")
    
    # Info: R² rendah bisa terjadi jika target sangat deterministic dari features
    if r2 < 0.2:
        log_warn(f"⚠️ R² rendah - target mungkin sangat deterministic atau constant")
        log_info(f"   MAPE: {mape:.2f}% (error rata-rata dalam %)")

    log_success(step)
    return model_reg, pred_reg


# ═══════════════════════════════════════════════════════════════════════════════
# 6. CLASSIFICATION MODEL — Random Forest Classifier
# ═══════════════════════════════════════════════════════════════════════════════
# def train_classifier(train, test):
#     step = "Classification Model — Gradient Boosting Classifier (optimized)"
#     log_start(step)

#     # Coba GBT untuk klasifikasi
#     gbt_clf = GBTClassifier(
#         featuresCol="features",
#         labelCol=TARGET_CLF,
#         predictionCol="stockout_prediction",
#         **GBT_PARAMS_CLF,
#     )

#     pipeline_clf = Pipeline(stages=[gbt_clf])
#     model_clf    = pipeline_clf.fit(train)
#     pred_clf     = model_clf.transform(test)

#     # Extract probability dari rawPrediction (untuk GBT - convert Vector to Array terlebih dahulu)
#     pred_clf = pred_clf.withColumn(
#         "stockout_probability",
#         vector_to_array(F.col("rawPrediction"))[1].cast("double")
#     )

#     # Evaluasi
#     auc = BinaryClassificationEvaluator(
#         labelCol=TARGET_CLF,
#         rawPredictionCol="rawPrediction",
#         metricName="areaUnderROC",
#     ).evaluate(pred_clf)

#     def eval_mc(metric):
#         return MulticlassClassificationEvaluator(
#             labelCol=TARGET_CLF,
#             predictionCol="stockout_prediction",
#             metricName=metric,
#         ).evaluate(pred_clf)

#     accuracy  = eval_mc("accuracy")
#     precision = eval_mc("weightedPrecision")
#     recall    = eval_mc("weightedRecall")
#     f1        = eval_mc("f1")

#     print("\n  ┌─────────────────────────────────┐")
#     print("  │  CLASSIFICATION METRICS (GBT)   │")
#     print(f"  │  AUC       : {auc:>8.4f}           │")
#     print(f"  │  Accuracy  : {accuracy:>8.4f}           │")
#     print(f"  │  Precision : {precision:>8.4f}           │")
#     print(f"  │  Recall    : {recall:>8.4f}           │")
#     print(f"  │  F1-Score  : {f1:>8.4f}           │")
#     print("  └─────────────────────────────────┘")

#     log_success(step)
#     return model_clf, pred_clf


def train_classifier(train, test):
    step = "Classification Model — Random Forest Classifier"
    log_start(step)

    # Inisialisasi Random Forest Classifier
    rf_clf = RandomForestClassifier(
        featuresCol="features",
        labelCol=TARGET_CLF,
        predictionCol="stockout_prediction",
        probabilityCol="stockout_probability_vec", # RF menghasilkan vector probability
        **RF_PARAMS_CLF
    )

    pipeline_clf = Pipeline(stages=[rf_clf])
    model_clf    = pipeline_clf.fit(train)
    pred_clf     = model_clf.transform(test)

    # Karena RF menghasilkan Vector [P(0), P(1)], kita ambil index 1 untuk probabilitas stockout
    pred_clf = pred_clf.withColumn(
        "stockout_probability",
        vector_to_array(F.col("stockout_probability_vec"))[1].cast("double")
    )

    # Evaluasi AUC
    auc = BinaryClassificationEvaluator(
        labelCol=TARGET_CLF,
        # Gunakan nama kolom yang Anda definisikan di RandomForestClassifier tadi
        rawPredictionCol="stockout_probability_vec", 
        metricName="areaUnderROC",
    ).evaluate(pred_clf)

    # Evaluasi Metrics lainnya
    def eval_mc(metric):
        return MulticlassClassificationEvaluator(
            labelCol=TARGET_CLF,
            predictionCol="stockout_prediction",
            metricName=metric,
        ).evaluate(pred_clf)

    print("\n  ┌─────────────────────────────────┐")
    print("  │  CLASSIFICATION METRICS (RF)    │")
    print(f"  │  AUC       : {auc:>8.4f}           │")
    print(f"  │  Accuracy  : {eval_mc('accuracy'):>8.4f}           │")
    print(f"  │  Precision : {eval_mc('weightedPrecision'):>8.4f}           │")
    print(f"  │  Recall    : {eval_mc('weightedRecall'):>8.4f}           │")
    print(f"  │  F1-Score  : {eval_mc('f1'):>8.4f}           │")
    print("  └─────────────────────────────────┘")

    log_success(step)
    return model_clf, pred_clf


# ═══════════════════════════════════════════════════════════════════════════════
# 7. FEATURE IMPORTANCE (dari Regressor)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_feature_importance(model_reg):
    step = "Feature Importance Visualization"
    log_start(step)

    # Ambil stage terakhir dari Pipeline (RF atau GBT)
    final_model = model_reg.stages[-1]
    
    # Extract importance (semua tree-based model punya featureImportances)
    importances = final_model.featureImportances.toArray()
    model_type = "RF Regressor"

    fi_df = pd.DataFrame({
        "Feature":    FEATURE_COLS,
        "Importance": importances,
    }).sort_values("Importance", ascending=False)

    print("\n  Feature Importance (dari {}):\n".format(model_type))
    print(fi_df.to_string(index=False))

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = sns.color_palette("RdYlGn", len(fi_df))
    bars = ax.barh(fi_df["Feature"][::-1], fi_df["Importance"][::-1], color=colors)
    
    # Tambah value label di bar
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width, bar.get_y() + bar.get_height()/2, f'{width:.4f}', 
                ha='left', va='center', fontsize=9, fontweight='bold')
    
    ax.set_xlabel("Importance Score", fontsize=12, fontweight='bold')
    ax.set_title(f"Feature Importance — {model_type}\n(reorder_point_new prediction)", 
                 fontsize=14, fontweight='bold')
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.set_xlim(0, max(fi_df["Importance"]) * 1.15)
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "feature_importance.png")
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    log_info(f"Plot disimpan : {path}")

    log_success(step)
    return fi_df


# ═══════════════════════════════════════════════════════════════════════════════
# 8. CONFUSION MATRIX
# ═══════════════════════════════════════════════════════════════════════════════
def plot_confusion_matrix(pred_clf):
    step = "Confusion Matrix Visualization"
    log_start(step)

    # Konversi ke pandas untuk visualisasi
    cm_pd = (
        pred_clf
        .select(TARGET_CLF, "stockout_prediction")
        .withColumnRenamed(TARGET_CLF, "actual")
        .withColumnRenamed("stockout_prediction", "predicted")
        .groupBy("actual", "predicted")
        .count()
        .toPandas()
    )

    # Pivot ke format matrix 2×2
    cm_pivot = cm_pd.pivot(index="actual", columns="predicted", values="count").fillna(0).astype(int)
    # Pastikan label 0 & 1 ada
    for lbl in [0, 1]:
        if lbl not in cm_pivot.index:   cm_pivot.loc[lbl] = 0
        if lbl not in cm_pivot.columns: cm_pivot[lbl]     = 0
    cm_pivot = cm_pivot.sort_index().sort_index(axis=1)

    fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(
        cm_pivot,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=["Aman (0)", "Risiko (1)"],
        yticklabels=["Aman (0)", "Risiko (1)"],
        ax=ax,
        linewidths=0.5,
        linecolor="white",
    )
    ax.set_xlabel("Predicted", fontsize=11)
    ax.set_ylabel("Actual",    fontsize=11)
    ax.set_title("Confusion Matrix — GBT Classifier (stockout_risk)", fontsize=12, fontweight="bold")
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "confusion_matrix.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    log_info(f"Plot disimpan : {path}")

    log_success(step)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. CORRELATION ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
def plot_correlation_heatmap(pred_clf):
    step = "Correlation Analysis Visualization"
    log_start(step)

    # Kolom yang akan dianalisis korelasinya: 5 fitur + 2 target
    corr_cols = FEATURE_COLS + [TARGET_CLF, TARGET_REG]

    # Ambil kolom yang tersedia di pred_clf (TARGET_REG mungkin tidak ada)
    available_cols = [c for c in corr_cols if c in pred_clf.columns]

    corr_pd = pred_clf.select(available_cols).toPandas()

    # Pastikan semua kolom numerik
    corr_pd = corr_pd.apply(pd.to_numeric, errors="coerce")

    corr_matrix = corr_pd.corr()

    # Label kolom lebih ringkas untuk tampilan
    label_map = {
        "stock_on_hand":           "Stock\non Hand",
        "avg_daily_demand":        "Avg Daily\nDemand",
        "procurement_lead_time":   "Lead\nTime",
        "supplier_risk":           "Supplier\nRisk",
        "inventory_turnover_rate": "Inventory\nTurnover",
        "stockout_risk":           "Stockout\nRisk",
        "reorder_point_new":       "ROP\nNew",
    }
    display_labels = [label_map.get(c, c) for c in corr_matrix.columns]

    fig, ax = plt.subplots(figsize=(8, 6))
    mask = np.zeros_like(corr_matrix, dtype=bool)
    np.fill_diagonal(mask, True)   # sembunyikan diagonal (nilai 1.0)

    sns.heatmap(
        corr_matrix,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        center=0,
        vmin=-1, vmax=1,
        square=True,
        linewidths=0.5,
        linecolor="white",
        xticklabels=display_labels,
        yticklabels=display_labels,
        ax=ax,
        cbar_kws={"shrink": 0.8, "label": "Pearson r"},
    )
    ax.set_title(
        "Correlation Heatmap — Fitur & Target\n(Pearson Correlation)",
        fontsize=13, fontweight="bold", pad=12,
    )
    plt.xticks(fontsize=9)
    plt.yticks(fontsize=9, rotation=0)
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "correlation_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log_info(f"Plot disimpan : {path}")

    # Print top korelasi dengan target stockout_risk ke console
    if TARGET_CLF in corr_matrix.columns:
        top_corr = (
            corr_matrix[TARGET_CLF]
            .drop(labels=[TARGET_CLF], errors="ignore")
            .abs()
            .sort_values(ascending=False)
        )
        print("\n  Korelasi fitur terhadap stockout_risk (|r|):")
        for feat, val in top_corr.items():
            raw_val = corr_matrix[TARGET_CLF][feat]
            print(f"    {feat:<28} : {raw_val:+.4f}")

    log_success(step)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. STOCKOUT RISK DISTRIBUTION BAR CHART
# ═══════════════════════════════════════════════════════════════════════════════
def plot_stockout_distribution(pred_clf):
    step = "Stockout Risk Distribution Visualization"
    log_start(step)

    # Hitung jumlah tiap kelas stockout_risk (0 dan 1)
    dist_pd = (
        pred_clf
        .groupBy("stockout_prediction")
        .count()
        .orderBy("stockout_prediction")
        .toPandas()
    )

    # Pastikan kedua label (0 dan 1) ada meskipun tidak muncul di prediksi
    all_labels = pd.DataFrame({"stockout_prediction": [0, 1]})
    dist_pd = all_labels.merge(dist_pd, on="stockout_prediction", how="left").fillna(0)
    dist_pd["count"] = dist_pd["count"].astype(int)

    labels      = ["Aman (0)", "Berisiko (1)"]
    counts      = dist_pd["count"].tolist()
    bar_colors  = ["#2ecc71", "#e74c3c"]   # hijau untuk aman, merah untuk berisiko
    total       = sum(counts)

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(labels, counts, color=bar_colors, width=0.45, edgecolor="white", linewidth=1.2)

    # Tambahkan label jumlah & persentase di atas tiap batang
    for bar, count in zip(bars, counts):
        pct = (count / total * 100) if total > 0 else 0
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + total * 0.01,
            f"{count:,}\n({pct:.1f}%)",
            ha="center", va="bottom",
            fontsize=11, fontweight="bold",
        )

    ax.set_xlabel("Stockout Risk", fontsize=12, fontweight="bold")
    ax.set_ylabel("Jumlah Produk", fontsize=12, fontweight="bold")
    ax.set_title(
        "Distribusi Prediksi Stockout Risk\n(GBT Classifier)",
        fontsize=14, fontweight="bold",
    )
    ax.set_ylim(0, max(counts) * 1.25)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "stockout_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log_info(f"Plot disimpan : {path}")

    log_success(step)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. BUILD FINAL OUTPUT DATAFRAME
# ═══════════════════════════════════════════════════════════════════════════════
def build_output(pred_reg, pred_clf):
    step = "Build Final Output DataFrame"  # section 10
    log_start(step)

    # Pilih kolom yang diperlukan dari masing-masing hasil prediksi
    df_reg = pred_reg.select("product_id", "optimal_rop_pred", REF_COL, TARGET_REG)
    df_clf = pred_clf.select(
        "product_id",
        "stockout_prediction",
        F.col("stockout_probability").cast("double"),
    )

    # Gabungkan berdasarkan product_id
    df_out = df_reg.join(df_clf, on="product_id", how="inner")

    # Kolom rekomendasi bisnis
    df_out = df_out.withColumn(
        "recommendation",
        F.when(F.col("stockout_prediction") == 1, F.lit("❗RESTOCK SEGERA"))
         .when(F.col("optimal_rop_pred") > F.col(REF_COL), F.lit("❕UPDATE ROP"))
         .otherwise(F.lit("✅ AMAN"))
    )

    # Urutkan kolom final
    df_out = df_out.select(
        "product_id",
        "optimal_rop_pred",
        "stockout_prediction",
        "stockout_probability",
        REF_COL,
        TARGET_REG,
        "recommendation",
    )

    log_info(f"Final output rows : {df_out.count()}")
    log_success(step)
    return df_out


# ═══════════════════════════════════════════════════════════════════════════════
# 10. TOP-10 PRODUK RISIKO TERTINGGI
# ═══════════════════════════════════════════════════════════════════════════════

def show_top10(df_out):
    step = "Top-10 Produk Risiko Tertinggi"
    log_start(step)

    # stockout_probability sudah berupa float (di-extract di train_classifier),
    # jadi tidak perlu vector_to_array() lagi
    top10_pd = (
        df_out
        .orderBy(F.col("stockout_probability").desc())
        .limit(10)
        .toPandas()
    )
    
    # Pastikan tipe data float sebelum round (handle kemungkinan data type issues)
    top10_pd["stockout_probability"] = pd.to_numeric(top10_pd["stockout_probability"], errors="coerce").round(4)
    top10_pd["optimal_rop_pred"]     = pd.to_numeric(top10_pd["optimal_rop_pred"], errors="coerce").round(2)
    top10_pd[REF_COL]                = pd.to_numeric(top10_pd[REF_COL], errors="coerce").round(2)

    print("\n  ╔══════════════════════════════════════════════════════════════════════════════╗")
    print("  ║          TOP-10 PRODUK DENGAN RISIKO STOCKOUT TERTINGGI                      ║")
    print("  ╚══════════════════════════════════════════════════════════════════════════════╝")
    print(top10_pd.to_string(index=False))

    log_success(step)
    return top10_pd


# ═══════════════════════════════════════════════════════════════════════════════
# 11. SIMPAN HASIL KE MINIO
# ═══════════════════════════════════════════════════════════════════════════════
def save_results(df_out):
    step = "Save Results to MinIO"
    log_start(step)

    df_out.write.mode("overwrite").parquet(OUTPUT_PATH)
    log_info(f"Hasil disimpan di : {OUTPUT_PATH}")

    log_success(step)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 70)
    print("  P03-A | Inventory Analytics — Modeling Pipeline")
    print("=" * 70)

    # ── Init Spark ──────────────────────────────────────────────────────────
    log_start("Spark Session Init")
    spark = make_spark()
    log_success("Spark Session Init")

    # ── 1. Data Preparation ─────────────────────────────────────────────────
    df = load_and_prepare(spark)
    df.cache()

    # ── 2. Train-Test Split ─────────────────────────────────────────────────
    train, test = split_data(df)

    # ── 3. Regression Model ─────────────────────────────────────────────────
    model_reg, pred_reg = train_regressor(train, test)

    # ── 4. Classification Model ─────────────────────────────────────────────
    model_clf, pred_clf = train_classifier(train, test)

    # ── 5. Feature Importance ───────────────────────────────────────────────
    plot_feature_importance(model_reg)

    # ── 6. Confusion Matrix ─────────────────────────────────────────────────
    plot_confusion_matrix(pred_clf)

    # ── 7. Correlation Analysis ─────────────────────────────────────────────
    plot_correlation_heatmap(pred_clf)

    # ── 8. Stockout Risk Distribution ───────────────────────────────────────
    plot_stockout_distribution(pred_clf)

    # ── 8. Build Final Output ───────────────────────────────────────────────
    df_out = build_output(pred_reg, pred_clf)
    df_out.cache()

    # ── 9. Top-10 Risiko Tertinggi ──────────────────────────────────────────
    show_top10(df_out)

    # ── 10. Save Results ─────────────────────────────────────────────────────
    save_results(df_out)

    # ── Cleanup ─────────────────────────────────────────────────────────────
    df.unpersist()
    train.unpersist()
    test.unpersist()
    df_out.unpersist()

    print("\n" + "=" * 70)
    print("  [FINISH] Modeling Pipeline selesai ✅")
    print("  ┌──────────────────────────────────────────────────────────────┐")
    print("  │  OPTIMISASI YANG DITERAPKAN                                  │")
    print("  │  ✓ Feature Scaling (StandardScaler) normalisasi input        │")
    print("  │  ✓ RF Regressor + RF  Classifier untuk robustness            │")
    print("  │  ✓ Hyperparameter tuning (maxDepth=4-6, minInstance=5)       │")
    print("  │  ✓ Subsampling (subsamplingRate=0.8) reduce overfitting      │")
    print("  │  ✓ MAPE metric untuk interpretasi error dalam %              │")
    print("  │                                                              │")
    print("  │  FITUR YANG DIGUNAKAN (5)                                    │")
    print("  │  • stock_on_hand                                             │")
    print("  │  • avg_daily_demand                                          │")
    print("  │  • procurement_lead_time                                     │")
    print("  │  • supplier_risk                                             │")
    print("  │  • inventory_turnover_rate                                   │")
    print("  │                                                              │")
    print("  │  OUTPUT TERSIMPAN DI:                                        │")
    print(f"  │  {OUTPUT_PATH:<56}    │")
    print("  │                                                              │")
    print("  │  VISUALISASI (folder: plots/)                                │")
    print("  │    • plots/feature_importance.png                            │")
    print("  │    • plots/confusion_matrix.png                              │")
    print("  │    • plots/correlation_heatmap.png                           │")
    print("  │    • plots/stockout_distribution.png                         │")
    print("  └──────────────────────────────────────────────────────────────┘")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()