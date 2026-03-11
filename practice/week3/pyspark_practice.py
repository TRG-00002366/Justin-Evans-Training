from pyspark.sql import SparkSession
from pyspark.sql.functions import col



spark = SparkSession.builder.appName("Context Comparison").master("local[*]").getOrCreate()


data = [("John", 30), ("Alice", 25), ("Bob", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)


df.show()