"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, sum as spark_sum, avg, count, min, max, countDistinct

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# TODO 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy

df.agg(
    spark_sum(col("amount")).alias("Total Sales"),
    avg(col("amount")).alias("Avg Sales"),
    min(col("amount")).alias("Minimum Sales"),
    max(col("amount")).alias("Maximum Sales")
).show()

# TODO 1b: Count the total number of sales transactions

#print(f"Number of sales transactions: {df.count()}")

df.agg(count(col("amount")).alias("Number of transactions")).show()

# TODO 1c: Count distinct categories

df.agg(
    countDistinct(col("category")).alias("Unique Categories")
).show()

# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

# TODO 2a: Total sales amount by category

df.groupBy("category").agg(
    spark_sum(col("amount")).alias("Sales By Category")
).show()

# TODO 2b: Average sale amount by month

df.groupBy("month").agg(
    avg(col("amount")).alias("Average Sales by Month")
).show()

# TODO 2c: Count of transactions by salesperson

df.groupBy("salesperson").agg(
    count(col("amount")).alias("Number of Transactions by Salesperson")
).show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# TODO 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!

df.groupBy("category").agg(
    count(col("amount")).alias("Number of Transactions by Dept"),
    spark_sum(col("amount")).alias("Total Sales"),
    avg(col("amount")).alias("Avg Sales"),
    max(col("amount")).alias("Maximum Sale")
).show()

# TODO 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)

df.groupBy("salesperson").agg(
    count(col("amount")).alias("Num of Transactions by Salesperson"),
    spark_sum(col("amount")).alias("Total Sales by Salesperson"),
    countDistinct(col("product")).alias("Total Unique Products")
).show()



# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# TODO 4a: Calculate total sales by month AND category

df.groupBy("month", "category").agg(
    spark_sum(col("amount")).alias("Total Sales by Category and Month")
    ).show()

# TODO 4b: Find the top salesperson by month (hint: use multi-column groupBy)
#I couldn't figure this out where it still showed the salespersons name
temp_df = df.groupBy("month", "salesperson").agg(
    expr("SUM(amount) AS top_sales")
    )
temp_df.groupBy("month").agg(
    max(col("top_sales")).alias("top_salesperson")
).show()


# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# TODO 5a: Find categories with total revenue > 2000

df.groupBy("category").agg(
    expr("SUM(amount) AS dept_sum")
).filter(col("dept_sum") > 2000).show()

# TODO 5b: Find salespeople who made more than 2 transactions

df.groupBy("salesperson").agg(
    expr("COUNT(salesperson) AS transaction_count")
).filter(col("transaction_count") > 2).show()

# TODO 5c: Find month-category combinations with average sale > 500

df.groupBy("month", "category").agg(
    expr("AVG(amount) AS avg_sales")
).filter(col("avg_sales") > 500).show()


# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# TODO 6a: Which category had the highest average transaction value?

print("Department with the highest everage transaction value is Electronics")


# TODO 6b: Who is the top salesperson by total revenue?

print("Top salesperson is Alice")

# TODO 6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column

print("Month with most diverse sales is 2023-01")


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
