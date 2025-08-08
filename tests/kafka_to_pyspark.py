from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, get_json_object
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType,
    BooleanType,
    ArrayType,
    LongType
)

def read_debezium_kafka_and_write_json(bootstrap_servers, topic, output_file):
    """
    Reads Debezium CDC messages from a Kafka topic using PySpark and writes them to a JSON lines file.

    Args:
        bootstrap_servers (str): Comma-separated list of Kafka bootstrap servers.
        topic (str): The Kafka topic containing Debezium CDC messages.
        output_file (str): The path to the output JSON lines file.
    """

    spark = (
        SparkSession
        .builder
        .appName("DebeziumKafkaToJSON")
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0')
        .getOrCreate()
    )

    debezium_key_schema = StructField("key", StructType([
        StructField("type", StringType(), True),
        StructField("fields", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("optional", BooleanType(), True),
            StructField("field", StringType(), True)
        ])), True),
        StructField("optional", BooleanType(), True),
        StructField("name", StringType(), True)
    ]), True)

    debezium_value_schema = StructField("value", StructType([
            StructField("before", StringType(), True), # or StructType based on your table
            StructField("after", StringType(), True), # or StructType based on your table
            StructField("source", StructType([
                StructField("version", StringType(), True),
                StructField("connector", StringType(), True),
                StructField("name", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("snapshot", StringType(), True),
                StructField("db", StringType(), True),
                StructField("sequence", StringType(), True),
                StructField("table", StringType(), True),
                StructField("server_id", IntegerType(), True),
                StructField("gtid", StringType(), True),
                StructField("file", StringType(), True),
                StructField("pos", IntegerType(), True),
                StructField("row", IntegerType(), True),
                StructField("thread", LongType(), True),
                StructField("query", StringType(), True)
            ]), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ]), True)


    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.truststore.location", "kafka/resources/kafka-ssl-truststore.p12")
        .option("kafka.ssl.truststore.password", "top-secret")
        .option("kafka.ssl.truststore.type", "PKCS12")
        .option("kafka.ssl.keystore.location", "kafka/resources/kafka-ssl-keystore.p12")
        .option("kafka.ssl.keystore.password", "top-secret")
        .option("kafka.ssl.keystore.type", "PKCS12")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # cast key and value from byte to string
    string_df = (
        df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    )

    payload_df = (
        string_df
        .select(from_json(col("key"), debezium_key_schema)
        .alias("debezium_key"))
    )

    # Write the extracted data to a JSON lines file
    query = (
        string_df
        .writeStream
        .outputMode("append")
        .format("json")
        .option("path", output_file)
        .option("checkpointLocation", "checkpoint")
        # .format("console")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    bootstrap_servers = "localhost:9092"  # Replace with your Kafka bootstrap servers
    topic = "tpch.public.orders"  # Replace with your Debezium topic
    output_file = "debezium_cdc"

    read_debezium_kafka_and_write_json(bootstrap_servers, topic, output_file)