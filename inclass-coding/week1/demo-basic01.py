from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, when
#Set up spark
spark = SparkSession.builder \
    .appName("Demo Data Frames") \
    .master("local[*]") \
    .getOrCreate()


data=[
    (1, "Alice", 34, "Engineering", 75000.0),
    (2, "Bob", 45, "Marketing", 65000.0),
    (3, "Charlie", 29, "Engineering", 80000.0),
    (4, "Diana", 31, "Sales", 55000.0),
    (5, "Eve", 38, "Marketing", 70000.0)
 
]

df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary"])
df.show()

# 2. create DF using schema

schema = StructType([
    #Field name, type of data, and if data is nullable
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

df_typed=spark.createDataFrame(data, schema)

df_typed.printSchema()

# 3. From the row object
from pyspark.sql import Row
row=[
    Row(id=6,name="Jasdhir",age=42,department="Training",salary=72000.0),
    Row(id=6,name="Singh",age=42,department="Sales",salary=62000.0)
]

df_rows=spark.createDataFrame(row)
df_rows.show()


#df.select("*")

df.select("name").show()

new_df = df.select("Name")
new_df.show()


df.select("name", "salary").show()

df.select(col("name")).show(3)

df.filter(df.age > 30).show()
#or
df.filter(col("salary") > 65000).show()

# and condition using & and OR condition using | equals using =

# isin

df.filter(col("department").isin("Marketing", "Sales")).show()

# Add column
df.withColumn("bonus", col("salary")*.10).show()


#Conditional Column
df.withColumn("salary_tier",
               when(col("salary") <= 55000.0, "Entry")   
               .when(col("salary") <= 70000.0, "Mid")
               .otherwise("Senior")
                ).show()



spark.stop()