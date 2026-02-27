from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col


# Create Spark Session

spark = SparkSession.builder \
    .appName("local[*]") \
    .getOrCreate()


employee_schema=StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])


# Create DF and Read CSV
df = spark.read.csv(
    "employees.csv",
    header = True,
    schema = employee_schema
)

#Show Data
df.show()





#Apply a Transformation
df_sal_gt_65 = df.filter(col("salary") > 65000)
df_sal_gt_65.show()

# Write to an output file
df_sal_gt_65.write.mode("overwrite").json("output/sal_gt_65")






spark.stop()