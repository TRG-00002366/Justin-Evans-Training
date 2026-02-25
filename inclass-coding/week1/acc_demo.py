from pyspark import SparkContext

sc=SparkContext("local[*]", "Accumulator Demo")


data=[
    "INFO 2026-02-25 Application started",
    "Warn 2026-02-25 Disk Space Low",
    "ERROR 2026-02-25 NullPointerException",
    "INFO 2026-02-25 User logged in",
    "ERROR 2026-02-25 NullPointerException",
    "INFO 2026-02-25 User logged in"    
]

# Create RDD
logs=sc.parallelize(data, 4)
# print(logs.getNumPartitions())
# print(logs.glom().collect())


# Create Accumulators
total_count=sc.accumulator(0)
error_count=sc.accumulator(0)

# Processing function
def process_logs(line):
    total_count.add(1)
    if "ERROR" in line:
        error_count.add(1)
    return line


# Apply a transformation
processed_logs=logs.map(process_logs)

# Collect the data
result=processed_logs.collect()

#Display the result
print(f"Total Logs: {total_count.value}")
print(f"Error Logs: {error_count.value}")