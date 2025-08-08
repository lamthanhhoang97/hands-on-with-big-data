import os
import pyspark
import pendulum
import pydeequ

# Python
from datetime import datetime
# Delta 
from delta import *
from delta.tables import DeltaTable, IdentityGenerator
# PySpark
import pyspark.sql.functions as F
from pyspark.sql.types import *
# Deequ
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *


DELTA_DATETIME_FOPMAT = "YYYY-MM-DD HH:mm:ss"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def convert_datetime_to_unix_timestamp(datetime_str):
    """
    Example input: 2018-01-01T00:00:00+00:00
    """
    dt_obj = pendulum.parse(datetime_str)
    return int(dt_obj.timestamp() * 1000)

def convert_to_datetime_obj(datetime_str):
    dt_obj = datetime.strptime(
        pendulum.parse(datetime_str).format(DELTA_DATETIME_FOPMAT),
        DEFAULT_DATETIME_FORMAT
    )
    return dt_obj

def build_incremental_parameters(
    spark,
    delta_input_path: str,
    delta_output_path: str,
    data_interval_end: datetime      
):
    """
    Change data feed feature in Delta Lake: <https://docs.delta.io/latest/delta-change-data-feed.html>
    If the table is empty, we get the changed data from version 0
    result = {
        'startingVersion': 0
    }
    
    - If startingTimestamp == endingTimestamp, it returns empty result => remove endingTimestamp if they are identical

    :return:
    {
        'startingTimestamp': ...,
        'endingTimestamp': ...
    }
    """
    result = {}

    # input table
    input_cdf_df = spark.sql(
        f"select min(_commit_timestamp), max(_commit_timestamp) from table_changes_by_path('{delta_input_path}', 0)"
    )
    if input_cdf_df.isEmpty():
        # the table is empty
        result = {
            'startingVersion': 0
        }
    else:
        input_cdf_df.show(truncate=False)
        commit_result = input_cdf_df.collect()
        first_commit_ts, last_commit_ts = commit_result[0][0], commit_result[0][1] # datetime.datetime
        if first_commit_ts == last_commit_ts:
            result = {
                'startingTimestamp': first_commit_ts
            }
        else:
            # get previous commit from current table
            current_cdf_df = spark.sql(f"select max(record_commit_timestamp) from delta.`{delta_output_path}`")
            prev_commit_ts = None
            if not current_cdf_df.isEmpty():
                prev_commit_ts = current_cdf_df.collect()[0][0] # datetime.datetime
            
            if prev_commit_ts is not None:
                result['startingTimestamp'] = max(first_commit_ts, prev_commit_ts)
            else:
                # current table is empty
                result['startingTimestamp'] = first_commit_ts
            
            result['endingTimestamp'] = min(data_interval_end, last_commit_ts)

            # check if start time = end time
            if result['startingTimestamp'] == result['endingTimestamp']:
                result.pop("endingTimestamp")


    # format the result: convert datetime to string
    # 2021-04-21 05:35:43
    if 'startingTimestamp' in result:
        result['startingTimestamp'] = datetime.strftime(result['startingTimestamp'], DEFAULT_DATETIME_FORMAT)

    if 'endingTimestamp' in result:
        result['endingTimestamp'] = datetime.strftime(result['endingTimestamp'], DEFAULT_DATETIME_FORMAT)

    return result

try:

    builder = (
        pyspark
        .sql.SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY'])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY'])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Deequ
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        # Logging
        # .config("spark.log.level", "DEBUG")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    sc = spark.sparkContext

    # define table
    DELTA_TABLE_PATH = 's3a://incremental-etl/dim_order'
    dt = (
        DeltaTable
        .createIfNotExists(spark)
        .addColumn("order_sk", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
        .addColumn("order_id", dataType=IntegerType())
        .addColumn("order_name", dataType=CharType(25))    
        .addColumn("order_state", dataType=CharType(25))        
        .addColumn("order_date", dataType=TimestampType())
        .addColumn("order_effective_date", dataType=TimestampType())
        .addColumn("record_active_flag", dataType=BooleanType())
        .addColumn("record_start_date", dataType=TimestampType())
        .addColumn("record_end_date", dataType=TimestampType())
        .addColumn("record_track_hash", dataType=IntegerType())
        # for incremental query
        .addColumn("record_commit_timestamp", dataType=TimestampType())
        .property('delta.enableChangeDataFeed', 'true')
        .location(DELTA_TABLE_PATH)
        .execute()
    )

    # read application arguments
    event_time_column = "event_ts_ms"
    LOOKBACK_WINDOW = 3

    to_ts_str = spark.conf.get("spark.app.to_timestamp", None)
    data_interval_end = convert_to_datetime_obj(to_ts_str)
    to_ts = pendulum.parse(to_ts_str)
    to_ts_ms = int(to_ts.timestamp() * 1000)

    from_ts_str = spark.conf.get("spark.app.from_timestamp", None)
    from_ts = pendulum.parse(from_ts_str)
    from_ts = (from_ts - from_ts.subtract(days=LOOKBACK_WINDOW)).start
    from_ts_ms = int(from_ts.timestamp() * 1000)    

    # create query parameters
    RAW_SALE_ORDER_DELTA_PATH = 's3a://incremental-etl/raw-sale-order'
    incremental_params = build_incremental_parameters(
        spark,
        delta_input_path=RAW_SALE_ORDER_DELTA_PATH,
        delta_output_path=DELTA_TABLE_PATH,
        data_interval_end=data_interval_end
    )

    read_options = {
        'readChangeFeed': 'true',
    }
    read_options.update(incremental_params)
    print(f"Incremental query parameters: {read_options}")   

    change_df = (
        spark
        .read.format("delta")
        .options(**read_options)
        .load('s3a://incremental-etl/raw-sale-order')
    )

    # _change_type | _commit_version | _commit_timestamp
    change_df = (
        change_df
        .filter(F.col("_change_type").isin(["insert", "update_postimage", "delete"]))
        .filter(F.col(event_time_column) < F.lit(to_ts_ms))
        .filter(F.col(event_time_column) >= F.lit(from_ts_ms))
    )

    # handle duplicated records
    change_df = (
        change_df
        .groupBy("id")
        .agg(
            F.max_by(F.struct("*"), "_commit_timestamp").alias("row")
        )
        .select("row.*")
    )
    change_df.show()

    # clean data
    transform_df = (
        change_df
        .select(
            "id",
            "name",
            "state",
            "date_order",
            "effective_date",
            "create_date",
            "event_ts", # event time
            "_commit_timestamp" # processing time
        )
        .withColumn("record_track_hash", F.hash(
            "name", "state", "date_order", "effective_date"
        ))
    )

    # quality checks
    if not transform_df.isEmpty():
        check = Check(spark, level=CheckLevel.Error, description="Quality check for stage_dim_order")
        check_run = (
            VerificationSuite(spark)
            .onData(transform_df)
            .addCheck(
                check
                .isUnique("id")
                .isComplete("id")
                .isContainedIn("state", allowed_values=["draft", "sent", "sale", "cancel"])
                .isComplete("date_order")
            )
            .run()
        )

        check_result_df = (
            VerificationResult.checkResultsAsDataFrame(spark, check_run)    
        )
        check_result_df.show(truncate=False)

        failed_check_df = (
            check_result_df
            .filter(F.col("constraint_status") == "Failure")
        )

        if not failed_check_df.isEmpty():
            raise Exception("Data quality checks failed!")

    # build scd type2 table
    existing_df = dt.toDF()

    # find matched rows to insert as new version
    rows_to_insert = (
        transform_df
        .join(
            existing_df.filter(existing_df.record_active_flag == True), # current state
            [
                existing_df.order_id == transform_df.id,
                existing_df.record_track_hash != transform_df.record_track_hash
            ], 
            "inner"
        )
        .select(
            transform_df.id,
            transform_df.name,
            transform_df.state,
            transform_df.date_order,
            transform_df.effective_date,
            transform_df.event_ts,
            transform_df.record_track_hash,
            transform_df._commit_timestamp
        )
    )
    rows_to_insert.show(truncate=False)
    print("Row to insert")

    # union all data
    rows_to_merge = (
        transform_df
        .select(
            "id",
            "name",
            "state",
            "date_order",
            "effective_date",
            "event_ts",
            "record_track_hash",
            "_commit_timestamp"
        )
        .withColumn("merge_key", F.col("id"))
        .union(
            rows_to_insert
            .withColumn("merge_key", F.lit(None)) # force to insert
        )
    )
    rows_to_merge.show(truncate=False)
    print("Row to merge")

    # merge data
    (
        dt.alias("target")
        .merge(
            rows_to_merge.alias("source"),
            condition="source.merge_key = target.order_id"
        )
        .whenMatchedUpdate(
            condition="target.record_active_flag = true and target.record_track_hash != source.record_track_hash",
            set={
                "target.record_active_flag": F.lit(False),
                "target.record_end_date": "source.event_ts",
                "target.record_commit_timestamp": "source._commit_timestamp"
            }
        )
        .whenNotMatchedInsert(
            values={
                "target.order_id": "source.id",
                "target.order_name": "source.name",
                "target.order_state": "source.state",
                "target.order_date": "source.date_order",
                "target.order_effective_date": "source.effective_date",
                "target.record_active_flag": F.lit(True),
                "target.record_start_date": "source.event_ts",
                "target.record_end_date": F.lit("9999-12-31"),
                "target.record_track_hash": "source.record_track_hash",
                "target.record_commit_timestamp": "source._commit_timestamp"
            }
        )
        .execute()
    )

    spark.stop()
    sc._gateway.jvm.System.exit(0)

except Exception as e:
    print(f"An error occurred: {e}")

    spark.stop()
    sc._gateway.jvm.System.exit(1)

    raise
