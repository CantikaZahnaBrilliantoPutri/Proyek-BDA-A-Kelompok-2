from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("BacaParquet") \
    .getOrCreate()

# Baca file parquet
path = "part-00000-9a75e407-9916-4233-9615-e97be8eb4d3c-c000.snappy.parquet"
df = spark.read.parquet(path)

# Menampilkan skema (nama kolom & tipe data)
df.printSchema()

# Menampilkan 5 baris pertama data
df.show(5)