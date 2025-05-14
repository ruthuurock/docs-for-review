from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, IntegerType
#spark-sql-kafka-0-10_2.12-3.1.2.jar,kafka-clients-3.3.1.jar,spark-streaming-kafka-0-10-assembly_2.12-3.1.2.jar,commons-pool2-2.11.1.jar
# Create a SparkSession
spark = SparkSession.builder.config("spark.jars", "C:\\Users\\muppa\\Downloads\\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\\Users\\muppa\\Downloads\\kafka-clients-3.3.1.jar,C:\\Users\\muppa\\Downloads\\spark-streaming-kafka-0-10-assembly_2.12-3.1.2.jar,C:\\Users\\muppa\\Downloads\\commons-pool2-2.11.1.jar") \
    .master("local").appName("KafkaStreamingToPostgreSQL").getOrCreate()
# Define the Kafka connection details
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "quickstart-events"

# Define the PostgreSQL connection details
postgres_host = "localhost"
postgres_port = "5432"
postgres_database = "postgres"
postgres_user = "postgres"
postgres_password = "postgres"
postgres_table = ""

# Define the schema of the Kafka message
kafka_schema = StructType().add("name", StringType()).add("age", IntegerType())

# Define a UDF to add the batch ID column
get_batch_id = udf(lambda _: spark.streams.active[0].id if len(spark.streams.active) > 0 else None, StringType())

# Define the batch processing logic
def write_batch_to_postgres(batch_df, batch_id):
    # Parse the JSON value and select specific fields
    parsed_df = batch_df.select(from_json(col("value").cast("string"), kafka_schema).alias("data")).select("data.*")

    # Add the batch ID column to the DataFrame
    parsed_df_with_batch_id = parsed_df.withColumn("batch_id", get_batch_id(col("name")))
    parsed_df_with_batch_id.show()
    # Write the transformed DataFrame to PostgreSQL
    # parsed_df_with_batch_id.write \
    #     .format("jdbc") \
    #     .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
    #     .option("dbtable", postgres_table) \
    #     .option("user", postgres_user) \
    #     .option("password", postgres_password) \
    #     .mode("append") \
    #     .save()

# Read data from Kafka as a streaming DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()
parsed_df = kafka_df.select(from_json(col("value").cast("string"), kafka_schema).alias("data")).select("data.*")
query=parsed_df.writeStream \
    .format("console") \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .start()
query.awaitTermination()
# Apply batch processing and write to PostgreSQL using foreachBatch
# kafka_df.writeStream \
#     .foreachBatch(write_batch_to_postgres) \
#     .start() \
#     .awaitTermination()

# Stop the Spark session

# query = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("console") \
#     .option("checkpointLocation", "path/to/HDFS/dir") \
#     .start()

# query.awaitTermination()
spark.stop()
