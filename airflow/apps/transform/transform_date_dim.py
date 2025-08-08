import pyspark

from delta import *

from pyspark.sql.types import *
from pyspark.sql.functions import to_date, lit
from pyspark.sql.functions import col, to_date, date_format, year, month, dayofweek, weekofyear, quarter
from pyspark.sql.types import DateType

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext # spark context

# Define start and end dates for the date dimension table
start_date = "1990-01-01"
end_date = "2020-12-31"

# Generate a sequence of dates
def date_range(start, end):
    """Generates a list of dates between start and end (inclusive)."""
    from datetime import datetime, timedelta

    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    delta = timedelta(days=1)
    dates = []
    while start_date <= end_date:
        dates.append(start_date.strftime("%Y-%m-%d"))
        start_date += delta
    return dates

date_list = date_range(start_date, end_date)

# Create a DataFrame from the list of dates
df = spark.createDataFrame([(date,) for date in date_list], ["date_str"])

# Convert the date string to DateType
df = df.withColumn("sk_datekey", to_date(col("date_str")))

# Extract date parts and create new columns
date_df = df.select(
    col("sk_datekey"),
    year(col("sk_datekey")).alias("year"),
    month(col("sk_datekey")).alias("month"),
    date_format(col("sk_datekey"), "MMMM").alias("month_name"),
    dayofweek(col("sk_datekey")).alias("day_of_week"),
    date_format(col("sk_datekey"), "EEEE").alias("day_name_long"),
    date_format(col("sk_datekey"), "EEE").alias("day_name_short"),
    weekofyear(col("sk_datekey")).alias("week_of_year"),
    quarter(col("sk_datekey")).alias("quarter"),
    date_format(col("sk_datekey"), "yyyyMMdd").cast("integer").alias("date_id"),
)
date_df.show(10)

output_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_date_dim"
(
    date_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("optimizeWrite", "true")
    .save(output_path)
)

spark.stop()
sc._gateway.jvm.System.exit(0)
