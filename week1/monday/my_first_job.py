from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # Step 2: Create some data
    sc = spark.sparkContext
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Step 3: Perform transformations
    
    even_squares = rdd \
        .filter(lambda x: x % 2 == 0) \
        .map(lambda x: x**2)

    # Step 4: Show results
    
    print(f"Even Squares: {even_squares.collect()}")



    # Sample data: (product, category, price, quantity)
    sales_data = [
        ("Laptop", "Electronics", 999.99, 5),
        ("Mouse", "Electronics", 29.99, 50),
        ("Desk", "Furniture", 199.99, 10),
        ("Chair", "Furniture", 149.99, 20),
        ("Monitor", "Electronics", 299.99, 15),
    ]

    # Create DataFrame with column names
    df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])



    df.show()

    print(f"Record Count: {df.count()}")

    df = df.withColumn("revenue", col("price")*col("quantity"))

    df.show()

    df.filter(col("category") == "Electronics").show()

    df.groupBy("category").agg(
        spark_sum(col("revenue")).alias("total_revenue")
    ).show()
    



    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()