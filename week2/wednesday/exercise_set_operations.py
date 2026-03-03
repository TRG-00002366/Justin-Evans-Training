"""
Exercise: Set Operations
========================
Week 2, Wednesday

Practice union, intersect, except operations on customer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Set Ops").master("local[*]").getOrCreate()

# January customers
jan_customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (4, "Diana", "diana@email.com")
], ["customer_id", "name", "email"])

# February customers  
feb_customers = spark.createDataFrame([
    (2, "Bob", "bob@email.com"),
    (4, "Diana", "diana@email.com"),
    (5, "Eve", "eve@email.com"),
    (6, "Frank", "frank@email.com")
], ["customer_id", "name", "email"])

# March customers (different column order!)
mar_customers = spark.createDataFrame([
    ("grace@email.com", "Grace", 7),
    ("henry@email.com", "Henry", 8),
    ("bob@email.com", "Bob", 2)  # Returning customer
], ["email", "name", "customer_id"])

print("=== Exercise: Set Operations ===")
print("\nJanuary Customers:")
jan_customers.show()
print("February Customers:")
feb_customers.show()
print("March Customers (different column order!):")
mar_customers.show()

# =============================================================================
# TASK 1: Union Operations (20 mins)
# =============================================================================

print("\n--- Task 1: Union ---")

# TODO 1a: Union January and February customers (keep duplicates)

union_of_customers = jan_customers.union(feb_customers)
union_of_customers.show()

# TODO 1b: Union January and February, then remove duplicates

union_of_customers_no_dup = union_of_customers.distinct()
union_of_customers_no_dup.show()

# TODO 1c: Try union with March customers - what happens? - an error since the column types do not match column by column
# Use unionByName to fix it
union_of_customers_march = union_of_customers_no_dup.unionByName(mar_customers).distinct()
union_of_customers_march.show()

# TODO 1d: How many unique customers do you have across all three months?
print(f"Number of Unique Customers From Jan-Mar: {union_of_customers_march.count()}")

# =============================================================================
# TASK 2: Intersect (15 mins)
# =============================================================================

print("\n--- Task 2: Intersect ---")

# TODO 2a: Find customers who appear in BOTH January AND February

intersect_of_customers = jan_customers.intersect(feb_customers)
intersect_of_customers.show()

# TODO 2b: Verify the result makes sense - who are the returning customers? - They are the only 2 customers which are in both January and February, so the answer makes sense
print("Bob and Diana are the only 2 customers who shopped in both Jan and Feb")

# =============================================================================
# TASK 3: Subtract/Except (15 mins)
# =============================================================================

print("\n--- Task 3: Subtract/Except ---")

# TODO 3a: Find customers in January who did NOT return in February
jan_feb_subtract = jan_customers.subtract(feb_customers)
jan_feb_subtract.show()

# TODO 3b: Find NEW customers in February (not in January)
feb_jan_subtract = feb_customers.subtract(jan_customers)
feb_jan_subtract.show()

# TODO 3c: Business question: What is the customer churn from Jan to Feb?
# Answer in a comment: 50% meaning that 50% did not return in Feb


# =============================================================================
# TASK 4: Distinct and DropDuplicates (15 mins)
# =============================================================================

print("\n--- Task 4: Deduplication ---")

# Combined data with duplicates
all_data = jan_customers.union(feb_customers)

# TODO 4a: Use distinct() to remove exact duplicate rows
all_data.distinct().show()

# TODO 4b: Use dropDuplicates() on email column only
# (Keep first occurrence of each email)
all_data.dropDuplicates(["email"]).show()


# TODO 4c: What is the difference between distinct() and dropDuplicates()?
# Answer in a comment: One just drops all duplicate rows, while one would drop duplicate rows
# drop duplicates is better when you a subset of columns duplicating is a bad thing


# =============================================================================
# CHALLENGE: Data Reconciliation (20 mins)
# =============================================================================

print("\n--- Challenge: Data Reconciliation ---")

# System A data (source)
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300)
], ["id", "name", "price"])

# System B data (target)
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price difference!
    (4, "Product D", 400)   # New product!
], ["id", "name", "price"])

# TODO 5a: Find exact matches between source and target
exact_matches = source.intersect(target)
exact_matches.show()

# TODO 5b: Find records in source but not in target (or different)
uniq_to_src = source.subtract(target)
uniq_to_src.show()

# TODO 5c: Find records in target but not in source (or different)
uniq_to_trgt = target.subtract(source)
uniq_to_trgt.show()

# TODO 5d: Create a reconciliation report showing:
# - Matched count
# - Source-only count
# - Target-only count
print(f"Match Count: {exact_matches.count()}, Source-only Count: {uniq_to_src.count()}, Target-only Count: {uniq_to_trgt.count()}")


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
