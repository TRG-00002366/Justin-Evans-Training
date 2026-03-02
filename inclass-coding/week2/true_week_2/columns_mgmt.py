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
    .appName("Column App") \
    .master("local[*]") \
    .getOrCreate()

# Use some data
data = [
    (1, "  Alice Smith  ", "alice@company.com", 75000, None),
    (2, "  Bob Johnson  ", "bob.j@company.com", 65000, "NY"),
    (3, "  Charlie Brown  ", "charlie@company.com", 80000, "CA"),
    (4, "  Diana Prince  ", None, 70000, "TX")
]

# Create DataFrame from the data and use column names

df = spark.createDataFrame(data, ["id", "name", "email", "salary", "state"])

#show data
df.show()


# # Add a column withColumn -- with a default value use lit()
# df.withColumn("country", lit("USA")).show()

# #Add a new column as bonus which calcs and displays 10% of salary
# df.withColumn("Bonus", col("salary")*.1).show()


# #Add a bool column with a value based on value of another column like if salary > 70000 col value = High Earner
# df.withColumn("High Earner", col("salary") > 70000).show()

# # Add a column based on value of another column, -- conditional column
# df.withColumn("Salary Tier",
# when(col("salary") < 65000, "Entry")
# .when(col("salary") < 75000, "Mid")
# .otherwise("Senior")
# ).show()

# String Ops

# Clean the data
# clean_df = df.withColumn("name", trim(col("name")))

# #Extra First Name
# half_complete = clean_df.withColumn("first_name", split(col("name"), " ")[0]).show()

# full_complete = clean_df.withColumn("last_name", split(col("name"), " ")[1]).show()

# #Create a new column based on email, the new col replaces @company.com with @mycompany.com
# #DIY
# df.withColumn(
#     "email",
#     regexp_replace(col("email"), r"@company\.com$", "@mycompany.com")
# ).show()


# # Handle nulls --  coalesce -- used to handle null values
# # replace NULL in email column with value N/A
# df.withColumn("email", coalesce(col("email"), lit("N/A"))).show()

#rename column
df.withColumnRenamed("name", "Full_name").show()

#rename multiple columns at once -- use toDF()
df.toDF("emp_id", "emp_name", "emp_email", "salary", "state").show()

#Drop / removing columns 
df.drop("name", "email").show()

#You can chain your operations
#df.withColumn().withColumn().withColumn()

# clean up
spark.stop()