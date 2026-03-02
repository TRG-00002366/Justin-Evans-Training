import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, split, trim, regexp_replace, coalesce
)
import re



def clear_screen():
    """Clears the terminal screen."""
    # Check the operating system name
    if os.name == 'nt':
        # Command for Windows
        _ = os.system('cls')
    else:
        # Command for Linux/macOS (posix, etc.)
        _ = os.system('clear')
 
clear_screen()

# set up
spark = SparkSession.builder \
    .appName("Demo Select & Filter") \
    .master("local[*]") \
    .getOrCreate()


data = [
    (1, "Alice", 34, "Engineering", 75000, "NY"),
    (2, "Bob", 45, "Marketing", 65000, "CA"),
    (3, "Charlie", 29, "Engineering", 80000, "NY"),
    (4, "Diana", 31, None, 55000, "TX"),
    (5, "Eve", 38, "Marketing", None, "CA")
]

df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary", "state"])

# df.show()


# View data based on column
#df.select("name", "Salary").show()



# df.select(col("name"), "Salary").show()


# # View data based on column(s) col, df attribute df.age, [] notation -> df[id]
# df.select(df["id"], df.name, col("department"), "salary").show()



# SQL expressions selectExpr

# df.selectExpr(
#     "name", "age", "salary * 0.10 as Bonus"
# ).show()


# #filtering -- filter the rows
# df.select("name", "age").filter(col("age") > 31).show()
# df.where(col("age") > 31).show()

# Complex Conditions

# Filter based on 2 columns And - Or
# df.filter((col("age") > 30) & (col("salary") >= 60000)).show()

# df.filter(col("state").isin(["NY", "CA"])).show()

# df.filter(col("age").between(30, 40)).show()

# df.filter(col("name").contains("ar")).show()

# df.filter(col("name").like("A%")).show()

df.filter(col("department").isNotNull()).show()


#clean up
spark.stop()
