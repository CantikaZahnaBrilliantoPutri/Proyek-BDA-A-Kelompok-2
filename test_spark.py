import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .getOrCreate()

data = [("A", 1), ("B", 2)]
df = spark.createDataFrame(data, ["nama", "nilai"])
df.show()