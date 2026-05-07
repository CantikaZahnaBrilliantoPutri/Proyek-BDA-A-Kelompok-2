from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("BacaParquet") \
    .getOrCreate()

# Baca file parquet
path = "part-00000-957436a1-c680-4499-9b3e-4f114472406f-c000.snappy.parquet"
df = spark.read.parquet(path)

# Menampilkan skema (nama kolom & tipe data)
df.printSchema()

# Menampilkan 5 baris pertama data
df.show(5)