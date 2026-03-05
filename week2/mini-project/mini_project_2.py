from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, coalesce, lit, when
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName("Mini Project 2").master("local[*]").getOrCreate()

# Task 1
print("Task 1")
rides = spark.read.csv("./data/rides.csv", header = True, inferSchema=True)
drivers = spark.read.csv("./data/drivers.csv", header=True, inferSchema=True)

print("-"*(60))

#Task 2
print("Task 2")
rides.show(5)
rides.printSchema()

drivers.show(5)
drivers.printSchema()

print("-"*(60))

#Task 3
print("Task 3")

rides.select("ride_id", "pickup_location", "dropoff_location", "fare_amount").show()

print("-"*(60))

#Task 4
print("Task 4")

rides.filter((col("distance_miles") > 5) & (col("ride_type") == "premium")).show()

print("-"*(60))

#Task 5
print("Task 5")

rides.withColumn("fare_per_mile", col("fare_amount") / col("distance_miles")).show()

print("-"*(60))

#Task 6
print("Task 6")

rides.drop("ride_type").show()

print("-"*(60))

#Task 7
print("Task 7")

rides \
    .withColumnRenamed("pickup_location", "start_area") \
    .withColumnRenamed("dropoff_location", "end_area").show()

print("-"*(60))

#Task 8
print("Task 8")

sum_by_ride_type = rides.groupBy("ride_type").agg(
    spark_sum(col("fare_amount")).alias("type_sum")
)

sum_by_ride_type.show()



print("-"*(60))

#Task 9
print("Task 9")


driv_ratings = rides.groupBy("driver_id").agg(
    avg(col("rating")).alias("driver_rating")
)

driv_ratings.show()

print("-"*(60))

#Task 10
print("Task 10")

driv_and_rides = drivers.join(
    rides,
    rides.driver_id==drivers.driver_id,
    "inner"
).drop(rides.driver_id)

driv_and_rides.show()

print("-"*(60))

#Task 11
print("Task 11")

peak = rides.filter((col("ride_date") > "2024-12-31") & (col("ride_date") < "2025-02-01"))

peak.show()

off_peak = rides.filter((col("ride_date") > "2025-1-31") & (col("ride_date") < "2025-03-01"))

off_peak.show()

merged_sets = peak.union(off_peak)
merged_sets.show()




print("-"*(60))

#Task 12
print("Task 12")

temp_view = rides.createOrReplaceTempView("rides")

spark.sql("SELECT * FROM rides ORDER BY fare_amount DESC LIMIT 3").show()


print("-"*(60))

#Requirement 1
print("Requirement 1")

rides.orderBy(col("fare_amount").desc(), col("distance_miles").asc()).show()

print("-"*(60))

#Requirement 2
print("Requirement 2")

null_ratings = rides.filter(col("rating").isNull())

print(f"Number of null ratings: {null_ratings.count()}")

rides.withColumn("rating", coalesce(col("rating"), lit(0.0))).show()

print("-"*(60))

#Requirement 3
print("Requirement 3")

rides.withColumn("ride_category", when(col("distance_miles") < 3, "short")
                                    .when(col("distance_miles") < 8, "medium")
                                    .otherwise("long")).show()

print("-"*(60))

#Requirement 4
# I may revisit this if I have time
# print("Requirement 4")

# window_res = Window.partitionBy(col("fare_amount")).orderBy(col("ride_date"))

#Requirement 5
if not os.path.exists("./Ride_Analytics_Results/driver_ratings.csv"):
    driv_ratings.write.csv("./Ride_Analytics_Results/driver_ratings.csv")

if not os.path.exists("./Ride_Analytics_Results/sum_by_ride_type.csv"):
    sum_by_ride_type.write.csv("./Ride_Analytics_Results/sum_by_ride_type.csv")

if not os.path.exists("./Ride_Analytics_Results/drivers_and_rides.parquet"):
    driv_and_rides.write.parquet("./Ride_Analytics_Results/drivers_and_rides.parquet")