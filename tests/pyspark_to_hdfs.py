import os

from pyspark.sql import SparkSession

#NOTE
os.environ['HADOOP_USER_NAME'] = 'hadoopuser'

# Initialize SparkSession
spark = (
    SparkSession
    .builder
    .appName("HDFSExample")
    .getOrCreate()
)

# HDFS Configuration (if needed, usually auto-detected)
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://namenode:8020") #Example, replace with your namenode address and port.

# 1. Writing data to HDFS
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

df.show()

hdfs_path = "hdfs://172.19.0.2:9000/user/hoang/people.parquet"  # Replace with your HDFS path
df.write.mode("overwrite").parquet(hdfs_path)

print(f"Data written to: {hdfs_path}")

# 2. Reading data from HDFS
try:
    read_df = spark.read.parquet(hdfs_path)
    read_df.show()
except Exception as e:
    print(f"Error reading from HDFS: {e}")

# Stop SparkSession
spark.stop()