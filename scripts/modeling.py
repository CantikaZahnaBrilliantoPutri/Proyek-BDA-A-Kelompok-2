"""
P03-A | Inventory Analytics — Modeling Script
==============================================
Model 1 : Random Forest Regressor  → prediksi reorder_point optimal
Model 2 : Random Forest Classifier → prediksi stockout_risk

Input  : s3a://datalake-kelompok2/gold/ml_ready/
Output : s3a://datalake-kelompok2/gold/modeling_results/
"""

import os
import logging

# Suppress py4j / spark INFO noise sebelum SparkSession dibuat
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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 0. CONSTANTS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
BUCKET          = "s3a://datalake-kelompok2"
INPUT_PATH      = f"{BUCKET}/gold/ml_ready/"
OUTPUT_PATH     = f"{BUCKET}/gold/modeling_results/"
# direktori lokal untuk simpan gambar
PLOT_DIR        = "plots"          

FEATURE_COLS = [
    "sales_velocity",
    "stock_on_hand",
    "avg_daily_demand",
    "procurement_lead_time",
    "supplier_risk",
    "order_buffer_index",
    "inventory_turnover_rate",
    "log_sales",
    "demand_to_stock_ratio",
]

TARGET_REG  = "reorder_point"
TARGET_CLF  = "stockout_risk"
REF_COL     = "reorder_level" # ROP lama dari dataset asli

RF_PARAMS   = dict(numTrees=100, maxDepth=10, seed=42)

os.makedirs(PLOT_DIR, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. LOGGING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
_STEP_DONE  = 0
_STEP_TOTAL = 10

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
    df = df.fillna(0)

    # VectorAssembler: gabungkan fitur jadi satu kolom "features"
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    df = assembler.transform(df)

    log_info(f"Schema preview:")
    df.printSchema()

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
    step = "Regression Model — Random Forest Regressor"
    log_start(step)

    rf_reg = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_REG,
        predictionCol="optimal_rop_pred",
        **RF_PARAMS,
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

    print("\n  ┌─────────────────────────────────┐")
    print("  │  REGRESSION METRICS             │")
    print(f"  │  MAE  : {mae:>10.4f}              │")
    print(f"  │  RMSE : {rmse:>10.4f}              │")
    print(f"  │  R²   : {r2:>10.4f}              │")
    print("  └─────────────────────────────────┘")

    log_success(step)
    return model_reg, pred_reg


# ═══════════════════════════════════════════════════════════════════════════════
# 6. CLASSIFICATION MODEL — Random Forest Classifier
# ═══════════════════════════════════════════════════════════════════════════════
def train_classifier(train, test):
    step = "Classification Model — Random Forest Classifier"
    log_start(step)

    rf_clf = RandomForestClassifier(
        featuresCol="features",
        labelCol=TARGET_CLF,
        predictionCol="stockout_prediction",
        probabilityCol="stockout_prob_vec",
        **RF_PARAMS,
    )

    pipeline_clf = Pipeline(stages=[rf_clf])
    model_clf    = pipeline_clf.fit(train)
    pred_clf     = model_clf.transform(test)

    # Ekstrak probabilitas class 1 (risiko stockout) - convert Vector ke Array terlebih dahulu
    pred_clf = pred_clf.withColumn(
        "stockout_probability",
        vector_to_array(F.col("stockout_prob_vec"))[1].cast("double")
    )

    # Evaluasi
    auc = BinaryClassificationEvaluator(
        labelCol=TARGET_CLF,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(pred_clf)

    def eval_mc(metric):
        return MulticlassClassificationEvaluator(
            labelCol=TARGET_CLF,
            predictionCol="stockout_prediction",
            metricName=metric,
        ).evaluate(pred_clf)

    accuracy  = eval_mc("accuracy")
    precision = eval_mc("weightedPrecision")
    recall    = eval_mc("weightedRecall")
    f1        = eval_mc("f1")

    print("\n  ┌─────────────────────────────────┐")
    print("  │  CLASSIFICATION METRICS         │")
    print(f"  │  AUC       : {auc:>8.4f}           │")
    print(f"  │  Accuracy  : {accuracy:>8.4f}           │")
    print(f"  │  Precision : {precision:>8.4f}           │")
    print(f"  │  Recall    : {recall:>8.4f}           │")
    print(f"  │  F1-Score  : {f1:>8.4f}           │")
    print("  └─────────────────────────────────┘")

    log_success(step)
    return model_clf, pred_clf


# ═══════════════════════════════════════════════════════════════════════════════
# 7. FEATURE IMPORTANCE (dari Regressor)
# ═══════════════════════════════════════════════════════════════════════════════
def plot_feature_importance(model_reg):
    step = "Feature Importance Visualization"
    log_start(step)

    # Ambil RF stage dari Pipeline
    rf_model     = model_reg.stages[-1]
    importances  = rf_model.featureImportances.toArray()

    fi_df = pd.DataFrame({
        "Feature":    FEATURE_COLS,
        "Importance": importances,
    }).sort_values("Importance", ascending=False)

    print("\n  Feature Importance (dari Regressor):")
    print(fi_df.to_string(index=False))

    # Plot
    fig, ax = plt.subplots(figsize=(9, 5))
    colors = sns.color_palette("Blues_d", len(fi_df))
    ax.barh(fi_df["Feature"][::-1], fi_df["Importance"][::-1], color=colors)
    ax.set_xlabel("Importance Score", fontsize=11)
    ax.set_title("Feature Importance — RF Regressor (reorder_point)", fontsize=13, fontweight="bold")
    ax.grid(axis="x", linestyle="--", alpha=0.5)
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "feature_importance.png")
    fig.savefig(path, dpi=150)
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

    # Konversi ke pandas hanya untuk visualisasi
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
    ax.set_title("Confusion Matrix — RF Classifier (stockout_risk)", fontsize=12, fontweight="bold")
    plt.tight_layout()

    path = os.path.join(PLOT_DIR, "confusion_matrix.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    log_info(f"Plot disimpan : {path}")

    log_success(step)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. BUILD FINAL OUTPUT DATAFRAME
# ═══════════════════════════════════════════════════════════════════════════════
def build_output(pred_reg, pred_clf):
    step = "Build Final Output DataFrame"
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

    # ── 7. Build Final Output ───────────────────────────────────────────────
    df_out = build_output(pred_reg, pred_clf)
    df_out.cache()

    # ── 8. Top-10 Risiko Tertinggi ──────────────────────────────────────────
    show_top10(df_out)

    # ── 9. Save Results ─────────────────────────────────────────────────────
    save_results(df_out)

    # ── Cleanup ─────────────────────────────────────────────────────────────
    df.unpersist()
    train.unpersist()
    test.unpersist()
    df_out.unpersist()

    print("\n" + "=" * 70)
    print("  [FINISH] Modeling Pipeline selesai ✅")
    print("  ┌──────────────────────────────────────────────────────────────┐")
    print("  │  Output tersimpan di:                                        │")
    print(f"  │  {OUTPUT_PATH:<56}    │")
    print("  │                                                              │")
    print("  │  Visualisasi tersimpan di folder: plots/                     │")
    print("  │    • plots/feature_importance.png                            │")
    print("  │    • plots/confusion_matrix.png                              │")
    print("  └──────────────────────────────────────────────────────────────┘")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()