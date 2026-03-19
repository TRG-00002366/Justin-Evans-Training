from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import random



spark = SparkSession.builder.appName("Context Comparison").master("local[*]").getOrCreate()


data = [("John", 30), ("Alice", 25), ("Bob", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)


df.show()


df_from_csv = spark.read.csv("./customers-100.csv", header=True)


df_from_csv.withColumn("Third of year subscribed", when(col("Subscription Date")[6:2] <= "04", "First third") 
                                            .when(col("Subscription Date")[6:2] <= "08", "Second third")
                                            .otherwise("Final third")
    ).show()


df_from_csv.withColumn("Month Subscribed", col("Subscription Date")[6:2]).show()



df_from_csv.withColumn("Pointless column", lit(random.randint(1,50))).show()

df_from_csv.show()