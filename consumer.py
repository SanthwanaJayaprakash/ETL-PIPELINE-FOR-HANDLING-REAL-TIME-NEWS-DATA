from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

try:
    # Initialize Spark session with Kafka, MongoDB, and S3 configurations
    spark = SparkSession.builder \
        .appName("KafkaToMongoDBAndS3") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.mongodb.spark:mongo-spark-connector_2.13:10.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/mydatabase.mycollection") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/mydatabase.mycollection") \
        .config("spark.hadoop.fs.s3a.access.key", "<access_key>") \
        .config("spark.hadoop.fs.s3a.secret.key", "<secret_key>") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2") \
        .getOrCreate()
    print("Spark session created successfully.")

    # Define schema for the incoming JSON data
    schema = StructType([
        StructField("author", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("timestamp", TimestampType())  # Added timestamp field
    ])

    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "test1") \
            .option("startingOffset", "earliest") \
            .load()
        print("Kafka stream loaded successfully.")
        
        
        StructField("timestamp", TimestampType())
        # Parse the JSON data from Kafka
        parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        try:
            # Write to MongoDB
            mongo_query = parsed_df \
                .writeStream \
                .foreachBatch(lambda df, epoch_id: (
                    print(f"Writing to MongoDB (Batch {epoch_id}):"),
                    df.show(truncate=False),
                    df.write.format("mongodb")
                        .mode("append")
                        .option("database", "mydatabase")
                        .option("collection", "mycollection")
                        .save()
                )) \
                .option("checkpointLocation", "/tmp/mongodb_checkpoint") \
                .start()
            print("MongoDB stream started successfully.")
        except Exception as e:
            print(f"Error starting MongoDB stream: {str(e)}")
            raise

        try:
            # Write to S3 as CSV
            s3_query = parsed_df \
                .writeStream \
                .foreachBatch(lambda df, epoch_id: (
                    print(f"Writing to S3 (Batch {epoch_id}):"),
                    df.show(truncate=False),
                    df.write
                        .mode("append")
                        .format("csv")
                        .option("header", "true")
                        .save("s3a://santhwana-newsapi-etl/news_streamed_output3/")
                )) \
                .option("checkpointLocation", "/tmp/s3_checkpoint") \
                .start()
            print("S3 stream started successfully.")
        except Exception as e:
            print(f"Error starting S3 stream: {str(e)}")
            raise

        # Await termination for both queries
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Error in Kafka streaming or data processing: {str(e)}")
except Exception as e:
    print(f"Error initializing Spark session: {str(e)}")