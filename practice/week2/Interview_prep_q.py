from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.appName("Find Duplicates").getOrCreate()

data = [
    ("Alice", "alice@email.com"),
    ("Bob", "bob@email.com"),
    ("Alice", "alice@email.com"),  # Duplicate
    ("Charlie", "charlie@email.com"),
    ("Alice", "alice@email.com")   # Duplicate
]

df = spark.createDataFrame(data, ["name", "email"])

# Method 1: Find which records are duplicated
duplicates = df.groupBy(df.columns) \
               .agg(count("*").alias("count")) \
               .filter(col("count") > 1)

print("Duplicate records:")
duplicates.show()

# Method 2: Show all duplicate rows
df_with_count = df.groupBy(df.columns).count()
duplicate_records = df.join(df_with_count.filter(col("count") > 1).drop("count"),
                           df.columns, "inner")