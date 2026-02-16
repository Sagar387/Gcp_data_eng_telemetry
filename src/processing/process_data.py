from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


# Configuration
BUCKET_NAME = "telemetry-lake-sagar"
PROJECT_ID = "pulse-stream-core"

def main():
    spark = SparkSession.builder \
        .appName("Pulse-Stream-Refinery") \
        .getOrCreate()
    print("Spark Session Started!")
    
    # Set bucket name
    BUCKET_NAME = "telemetry-lake-sagar"
    
    # Define schema 
    schema = StructType([
        StructField("event_uuid", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("location", StringType(), True)
    ])
        
    # Read the telemetry data (Data swamp currently)
    # Read the raw json files in the cloud storage bucket
    raw_df = spark.read \
        .schema(schema) \
        .json(f"gs://{BUCKET_NAME}/raw_telemetry/*.json")
    
    print(f"Ingested {raw_df.count()} raw records.")
    
    # Read the Reference Json file in Cloud Storage
    athlete_df = spark.read \
        .option("multiline", "true") \
        .json(f"gs://{BUCKET_NAME}/reference_data/athlete.json")
        
    # Data Cleaning Bronze layer
    
    # Dropping duplicates
    clean_df = raw_df.dropDuplicates(["event_uuid"])
    
    # Handle NUll (sensor errors)
    clean_df = clean_df.filter(col("heart_rate").isNotNull())
    
    # Clean up outliers (heart rate over 220)
    clean_df = clean_df.withColumn("heart_rate", when(col("heart_rate")>220, 220).otherwise(col("heart_rate")))
    
    # Adding names and countries to the telemetry
    # Joining with the same athlete id
    final_df = clean_df.join(athlete_df, "athlete_id","right")
    
    # Checking
    final_df.select("name", "country", "heart_rate", "speed", "timestamp").show(5)
    
    # Write : save this to silver layer (parquet format as it is faster than json)
    final_df.write \
        .mode("overwrite") \
        .parquet(f"gs://{BUCKET_NAME}/silver_telemetry/")
        
    print("Data has successfully been writen to silver layer")
    
    #End spark session
    spark.stop()
    
if __name__ == "__main__":
    main()
    