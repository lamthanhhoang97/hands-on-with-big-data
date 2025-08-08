import os

from pyspark.sql import SparkSession

#NOTE
os.environ['HADOOP_USER_NAME'] = 'hadoopuser'

spark = SparkSession.builder.appName("ParquetRowCount").getOrCreate()
df = (
    spark.read
    .option('compression', 'snappy')
    .parquet("hdfs://localhost:9000/user/hadoopuser/raw/raw_lineitem/part-00179-b5d24f62-9378-4413-b3d5-51cdd82d6c81-c000.snappy.parquet")
)
row_count = df.count()

print(f"Row count: {row_count}")

spark.stop()