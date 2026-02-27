from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, avg, count, min, max, collect_list, collect_set, variance, expr,
    countDistinct, round as spark_round
)


spark = SparkSession.builder.appName("Aggregations Demo").getOrCreate()


# Sample sales data
sales_data = [
    ("2023-01", "Electronics", "Laptop", 1200, "NY"),
    ("2023-01", "Electronics", "Phone", 800, "NY"),
    ("2023-01", "Electronics", "Tablet", 500, "CA"),
    ("2023-01", "Clothing", "Jacket", 150, "NY"),
    ("2023-01", "Clothing", "Shoes", 100, "CA"),
    ("2023-02", "Electronics", "Laptop", 1300, "TX"),
    ("2023-02", "Electronics", "Phone", 850, "NY"),
    ("2023-02", "Clothing", "Jacket", 175, "CA"),
    ("2023-02", "Clothing", "Pants", 80, "TX")
]
#Create DataFrame
df = spark.createDataFrame(sales_data, ["month", "category", "product", "amount", "state"])


df.groupBy("category").avg("amount").show()

df.show()


print(f"Total Record : {df.count()}")

# use basic agg( without groupby)

df.agg(
    count("*").alias("total sales"),
    sum("amount").alias("total revenue"),
    min("amount").alias("min amount")
).show()

# groupby

df.groupBy("category").count().show()

print("Group By Product")
df.groupBy("product").sum("amount").show()



#Group by using agg() for complex agg

df.groupBy("category").agg(
    count("*").alias("Total sales"),
    sum("amount").alias("Total Revenue"),
    min("amount").alias("Min Salary")
).show()