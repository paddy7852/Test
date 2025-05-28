from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestTransform") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("file:///mnt/c/Users/ganga/OneDrive/Documents/test/cancer.csv", header=True, inferSchema=True)





# COMMAND ----------

# display function will visualize your dataset in a beautiful way. This is databricks notebook function which will not work in your spark scripts.
df.show(10)

