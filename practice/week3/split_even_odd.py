from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Practice demo").master("local[*]").getOrCreate()


sc = spark.sparkContext

df1 = spark.read.csv("./customers-100.csv", header=True)

df1.show()

df1.orderBy(col("Subscription Date").desc()).show()

df1.withColumn("year", col("Subscription Date")[0:4]).drop(col("Customer Id")).show()




spark.stop()

