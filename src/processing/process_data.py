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
    print("🚀 Spark Session Started!")
    
    # Define schema 
    schema = StructType([
        StructField("event_uuid", StringType(), True),
        StructField("athlete_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("location", StringType(), True)
    ])
        
    # Read the raw json files in the cloud storage bucket
    raw_df = spark.read \
        .schema(schema) \
        .json(f"gs://{BUCKET_NAME}/raw_telemetry/*.json")
    
    print(f"📥 Ingested {raw_df.count()} raw records.")
    
    # Read the Reference Json file in Cloud Storage
    athlete_df = spark.read \
        .option("multiline", "true") \
        .json(f"gs://{BUCKET_NAME}/reference_data/athlete.json")
        
    # ==========================================
    # 🚨 THE DEAD LETTER QUEUE (DLQ) ROUTING
    # ==========================================
    
    # 1. Identify Bad Data (Missing critical IDs or sensor failures)
    dlq_df = raw_df.filter(col("heart_rate").isNull() | col("event_uuid").isNull())
    
    dlq_count = dlq_df.count()
    if dlq_count > 0:
        print(f"⚠️ Found {dlq_count} corrupted records. Routing to DLQ...")
        # Save bad data to a separate folder for debugging
        dlq_df.write \
            .mode("append") \
            .json(f"gs://{BUCKET_NAME}/dlq_telemetry/")
    else:
        print("✅ No corrupted records found. DLQ is empty.")

    # 2. Keep only the Good Data for the Silver Layer
    valid_df = raw_df.filter(col("heart_rate").isNotNull() & col("event_uuid").isNotNull())
    
    # ==========================================
    # 🥈 SILVER LAYER TRANSFORMATIONS
    # ==========================================
    
    # Dropping duplicates
    clean_df = valid_df.dropDuplicates(["event_uuid"])
    
    # Clean up outliers (heart rate over 220)
    clean_df = clean_df.withColumn("heart_rate", when(col("heart_rate") > 220, 220).otherwise(col("heart_rate")))
    
    
    bad_df = raw_df.filter(
        col("heart_rate").isNull() | 
        col("event_uuid").isNull() |
        (col("heart_rate") > 220)
    )
    bad_df.write.mode("append").json(f"gs://{BUCKET_NAME}/dlq/")
    print(f"Quarantined {bad_df.count()} records to DLQ.")

    # Adding names and countries to the telemetry
    final_df = clean_df.join(athlete_df, "athlete_id", "left")
    
    # Checking
    print("Previewing Silver Layer Data:")
    final_df.select("name", "country", "heart_rate", "speed", "timestamp").show(5)
    
    # Write : save this to silver layer (parquet format as it is faster than json)
    final_df.write \
        .mode("overwrite") \
        .parquet(f"gs://{BUCKET_NAME}/silver_telemetry/")
        
    print("💾 Data has successfully been written to Silver Layer (Parquet).")
    
    # End spark session
    spark.stop()
    
if __name__ == "__main__":
    main()