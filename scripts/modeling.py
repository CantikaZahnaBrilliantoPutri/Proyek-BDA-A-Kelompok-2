from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator

# =========================
# 1. SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("Modeling")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-kelompok2:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# =========================
# 2. LOAD GOLD DATA
# =========================
df = spark.read.parquet("s3a://datalake-kelompok2/gold/gold_features")

print("Total data:", df.count())

# =========================
# 3. FEATURE SELECTION
# =========================
features = [
    "initial_stock_quantity",
    "stock_on_hand",
    "total_sales",
    "active_days",
    "daily_sales_velocity",
    "actual_lead_time",
    "avg_lead_time_days",
    "procurement_lead_time"
]

# filter fitur yang ada
features = [f for f in features if f in df.columns]

df = df.fillna(0)

# vector assembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df)

# =========================
# 4. TRAIN TEST SPLIT
# =========================
train, test = df.randomSplit([0.8, 0.2], seed=42)

# =========================
# 5. CLASSIFICATION MODEL
# =========================
clf = RandomForestClassifier(
    featuresCol="features",
    labelCol="stockout_risk",
    numTrees=100
)

model_clf = clf.fit(train)
pred_clf = model_clf.transform(test)

evaluator_clf = BinaryClassificationEvaluator(
    labelCol="stockout_risk"
)

auc = evaluator_clf.evaluate(pred_clf)

print("\n=== CLASSIFICATION METRICS ===")
print("AUC:", auc)

# =========================
# 6. REGRESSION MODEL
# =========================
reg = RandomForestRegressor(
    featuresCol="features",
    labelCol="reorder_point",
    numTrees=100
)

model_reg = reg.fit(train)
pred_reg = model_reg.transform(test)

evaluator_mae = RegressionEvaluator(
    labelCol="reorder_point",
    predictionCol="prediction",
    metricName="mae"
)

evaluator_r2 = RegressionEvaluator(
    labelCol="reorder_point",
    predictionCol="prediction",
    metricName="r2"
)

mae = evaluator_mae.evaluate(pred_reg)
r2  = evaluator_r2.evaluate(pred_reg)

print("\n=== REGRESSION METRICS ===")
print("MAE:", mae)
print("R2 :", r2)

# =========================
# 7. FEATURE IMPORTANCE
# =========================
print("\n=== FEATURE IMPORTANCE ===")
importances = model_clf.featureImportances

for i, f in enumerate(features):
    print(f"{f}: {importances[i]:.4f}")

spark.stop()