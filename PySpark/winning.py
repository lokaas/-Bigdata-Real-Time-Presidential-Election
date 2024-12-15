from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType, IntegerType, LongType

# Spark session
spark = SparkSession.builder \
    .appName("PresidentialElectionProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

# Define the schema for the voter data
schema = StructType() \
    .add("id", LongType()) \
    .add("gender", StringType()) \
    .add("age", IntegerType()) \
    .add("state", StringType()) \
    .add("decision", StringType())

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "voter_data") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggregate vote counts by candidate (decision) to determine the leader
vote_counts = df.groupBy("decision").count()

# Write the aggregated vote counts to the console to see which candidate is leading
query = vote_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

