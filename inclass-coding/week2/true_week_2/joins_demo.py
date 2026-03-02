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




# Create sample DataFrames

employees = spark.createDataFrame([

    (1, "Alice", 101),

    (2, "Bob", 102),

    (3, "Charlie", 103),

    (4, "Diana", None)  # No department

], ["emp_id", "emp_name", "dept_id"])
 
departments = spark.createDataFrame([

    (101, "Engineering", "Building A"),

    (102, "Marketing", "Building B"),

    (104, "Sales", "Building C")  # No employees

], ["dept_id", "dept_name", "location"])
 
# employees.show()
# departments.show()

# # 1. inner -- matching rows, most common
# inner_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     "inner"
# )
# inner_result.show()

# # 2. Outer Join -- 
# # 2-A. Left - Keeps all from left 
# left_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     "left"
# )
# left_result.show()


# #2-B. Right - Keeps all from right
# right_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     "right"
# )
# right_result.show()


# #2-C. Full - Keeps all from left and right
# outer_result = employees.join(
#     departments,
#     employees.dept_id == departments.dept_id,
#     "outer"
# )
# outer_result.show()


# 3. Cross - Cartesian join product
# cross_result = employees.crossJoin(departments)
# hours = spark.createDataFrame([(i,) for i in range(1, 13)], ['hours'])
# minutes = spark.createDataFrame([(i,) for i in range(60)], ['minutes'])
# cross_result = hours.crossJoin(minutes).show(300)

# SEMI Join -- shows where the left table has a matching value in the right table, it does not show columns from right table
semi_result = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "semi"
)
semi_result.show()


#ANTI Join results where the left table does not have a matching value, does not show values in right table
anti_result = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "anti"
)
anti_result.show()




spark.stop()