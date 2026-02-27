from pyspark.sql import SparkSession

spark=SparkSession.builder \
    .master("local[*]") \
    .appName("First Demo") \
    .getOrCreate()


data=[("Seth", 25), ("Justin", 26), ("Juan", 34), ("Anas", 27)]
df=spark.createDataFrame(data, ["name","age"])


# For RDD operations, access SparkContext
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3])

# You can convert between RDD and DataFrame
df_from_rdd = rdd.map(lambda x: (x,)).toDF(["value"])
rdd_from_df = df.rdd

df_from_rdd.show()









#sc=spark.SparkContext
print("-"*50)
print(f"App Name : {spark.sparkContext.appName}" )
print(f"Spark Version : {spark.version}")
print(f"Master : {spark.sparkContext.master}")
print("-"*50)

print("="*60)
data=[("Seth", 25), ("Justin", 26), ("Juan", 34), ("Anas", 27)]

#This data2 list would not work for a dataframe, but works just fine for a rdd
data2=[("Seth, 25"), ("Justin", 26), ("Juan", 34), ("Anas", 27)]
df=spark.createDataFrame(data, ["name","age"])
data_rdd=spark.sparkContext.parallelize(data2)
df.show()

df.createOrReplaceTempView("people")
result = spark.sql("SELECT name, age FROM people WHERE age > 30")
result.show()

print(data_rdd.collect())
df.printSchema()
print("="*60)

# print("*"*60)
# spark2=SparkSession.builder.appName("Spark 2").getOrCreate()
# print(f"Are spark and spark2 same object : {spark is spark2}")
# print("*"*60)

print("*"*60)
print("Configuration Settings")
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
spark.conf.set("spark.sql.shuffle.partitions", 50)
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("*"*60)


spark.stop()

