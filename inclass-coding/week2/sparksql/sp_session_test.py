from pyspark.sql import SparkSession
import os

def create_spark_session(app_name: str, environment: str = "development") -> SparkSession:
    """
    Create a SparkSession with environment-appropriate configuration.
    
    Args:
        app_name: Name of the Spark application
        environment: One of 'development', 'testing', 'production'
    
    Returns:
        Configured SparkSession
    """
    
    builder = SparkSession.builder.appName(app_name)
    
    # Base configurations for all environments
    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    if environment == "development":
        # Local development settings
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.ui.showConsoleProgress", "true")
    
    elif environment == "testing":
        # Testing settings (smaller resources)
        builder = builder \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4")
    
    elif environment == "production":
        # Production settings (cluster mode)
        builder = builder \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    return builder.getOrCreate()


# Usage
if __name__ == "__main__":
    env = os.getenv("SPARK_ENV", "development")
    spark = create_spark_session("MyDataPipeline", env)
    
    try:
        # Your application logic here
        df = spark.range(1000)
        print(f"Created DataFrame with {df.count()} rows")
        
        # Check configuration
        print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    
    finally:
        # Always stop the session
        spark.stop()