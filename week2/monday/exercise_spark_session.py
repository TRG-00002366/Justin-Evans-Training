"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday

Complete the TODOs below to practice creating and configuring SparkSession objects.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================

# TODO 1a: Create a SparkSession with:
#   - App name: "MyFirstSparkSQLApp"
#   - Master: "local[*]"
# HINT: Use SparkSession.builder.appName(...).master(...).getOrCreate()

spark = SparkSession.builder \
    .appName("MyFirstSparkSQLApp") \
    .master("local[*]") \
    .getOrCreate()

# TODO 1b: Print the following information:
#   - Spark version
#   - Application ID
#   - Default parallelism
# HINT: Access these via spark.version, spark.sparkContext.applicationId, etc.

print("=== Task 1: Basic SparkSession ===")
print(f"Spark Version: {spark.version}")
print(f"Spark Parallelism: {spark.sparkContext.defaultParallelism}")
print(f"App ID: {spark.sparkContext.applicationId}")


# TODO 1c: Create a simple DataFrame with 3 columns and 5 rows
# to verify your session works

data = [("Todd", 24, "NC"),
("Henry", 27, "ND"),
("Barney", 32, "CA"),
("Jane", 34, "VA"),
("Justin", 22, "NC"),
]  
columns = ["name", "age", "state"] 
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()


# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

# TODO 2a: Print the value of spark.sql.shuffle.partitions
# HINT: Use spark.conf.get("spark.sql.shuffle.partitions")


print(f"Shuffle partitions: {spark.conf.get("spark.sql.shuffle.partitions")}")  # Complete this


# TODO 2b: Print at least 3 other configuration values
# # Some options: spark.driver.memory, spark.executor.memory, spark.sql.adaptive.enabled
#print(f"Executor Heartbeat Interval: {spark.conf.get("spark.executor.memory", "default_value_if_not_set")}")
print(f"App id: {spark.conf.get("spark.app.id")}")
print(f"App name: {spark.conf.get("spark.app.name")}")
print(f"App name: {spark.conf.get("spark.default.parallelism", "This has not been set")}")



# TODO 2c: Try changing spark.sql.shuffle.partitions at runtime
# Does it work? Add a comment explaining what happens.
spark.conf.set("spark.sql.shuffle.partitions", "100")
#It works since it can be applied to transactions done after this point


# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# TODO 3a: Create another reference using getOrCreate with a DIFFERENT app name
spark2 = SparkSession.builder.appName("DifferentName").getOrCreate()


# TODO 3b: Check which app name is actually used
print(f"spark app name: {spark.sparkContext.appName}")
print(f"spark2 app name: {spark2.sparkContext.appName}")


# TODO 3c: Are spark and spark2 the same object? Check with 'is' operator
spark_bool = (spark is spark2)
print(f"Both spark sessions are the same: {spark_bool}")

# TODO 3d: EXPLAIN IN A COMMENT: Why does getOrCreate() behave this way?
# Your explanation:
# get or create got the spark context used by spark for spark2 since it was already created
# you can do this since you can have multiple spark sessions defined
# but you cant have multiple spark contexts defined, so it is shared


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

# TODO 4a: Stop the SparkSession properly
spark.stop()


# TODO 4b: Verify the session has stopped
# HINT: Check spark.sparkContext._jsc.sc().isStopped() before stopping
if SparkSession.getActiveSession() is not None:
    spark2.stop()
else:
    print("This executes if session has been stopped already")

# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================

# Stretch 1: Create a helper function that builds a SparkSession with your
# preferred default configurations

def create_my_spark_session(app_name, shuffle_partitions=100):
    """
    Creates a SparkSession with custom defaults.
    
    Args:
        app_name: Name of the Spark application
        shuffle_partitions: Number of shuffle partitions (default: 100)
    
    Returns:
        SparkSession object
    """
    # TODO: Implement this function
    pass


# Stretch 2: Enable Hive support
# HINT: Use .enableHiveSupport() in the builder chain
# Note: This may fail if Hive is not configured - that's okay!


# Stretch 3: List all configuration options
# HINT: spark.sparkContext.getConf().getAll() returns all settings
