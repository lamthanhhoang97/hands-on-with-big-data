import pyspark
import pydeequ
import re
import os

from delta import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *


def create_deequ_checks(spark, table_name, table_config):
    """Creates Deequ checks based on a configuration file."""
    check = Check(spark, CheckLevel.Error, f"Quality Check for table: {table_name}")

    for check_def in table_config.get("checks", []):
        column = check_def.get("column")
        check_type = check_def.get("type")
        params = check_def.get("params", {})

        if check_type == "is_unique":
            check = check.isUnique(column)
        elif check_type == "has_uniqueness":
            check = check.hasUniqueness(column, lambda x: f"{str(column)} is unique")
        elif check_type == "non_negative":
            check = check.isNonNegative(column)
        elif check_type == "contains":
            check = check.isContainedIn(column, params["allowed_values"])
        elif check_type == "is_less_than":
            check = check.isLessThan(column[0], column[1])

    return check

def get_valid_dfs_name(original_string):
    result = re.sub(r':', '-', original_string).replace('+00:00', 'Z')
    return result

def main():

    builder = (
        pyspark
        .sql.SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Deequ
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Logging
        # .config("spark.log.level", "DEBUG")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    sc = spark.sparkContext

    # get arguments
    table_name = spark.conf.get("spark.app.table_name")
    dag_id = spark.conf.get("spark.app.dag_id")
    run_id = get_valid_dfs_name(spark.conf.get("spark.app.run_id"))

    # read data
    s3_path = f's3a://raw/{table_name}.tbl'
    df = spark.read.csv(
        s3_path,
        sep="|",
        header=False,
        inferSchema=True
    )

    # rename the columns
    table_name_2_cols = {
        'nation': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment', 'n_dummy'],
        'region': ['r_regionkey', 'r_name', 'r_comment', 'r_dummy'],
        'supplier': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment', 's_dummy'],
        'customer': ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment', 'c_dummy'],
        'part': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment', 'p_dummy'],
        'partsupp': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment', 'ps_dummy'],
        'orders': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment', 'o_dummy'],
        'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment', 'l_dummy']
    }
    new_columns = table_name_2_cols.get(table_name, [])
    df = df.toDF(*new_columns)

    # drop the dummy column
    df = df.drop(new_columns[-1])
    df.printSchema()
    print(f"Number of partitions: {df.rdd.getNumPartitions()}")

    if table_name == 'lineitem':
        # repartition
        estimated_partitions = min(200, df.rdd.getNumPartitions())
        df = df.repartition(estimated_partitions)
        
    # df.show(10)

    # data profiling
    # profiling_result = (
    #     ColumnProfilerRunner(spark)
    #     .onData(df)
    #     .run()
    # )

    # print(f"Profiling result for {table_name}")
    # for col, profile in profiling_result.profiles.items():
    #     print("="*5, col, "="*5)
    #     print(profile)

    # data quality checks
    data_quality_config = {
        # raw layers
        "orders": {
            "checks": [
                {"column": "o_orderkey", "type": "is_unique"},
                {"column": "o_totalprice", "type": "non_negative"},
                {"column": "o_orderpriority", "type": "contains", "params": {"allowed_values": ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]}},
                {"column": "o_orderstatus", "type": "contains", "params": {"allowed_values": ["O", "F", "P"]}},
            ]
        },
        "nation": {
            "checks": [            
                {"column": "n_nationkey", "type": "is_unique"},
                {"column": "n_nationkey", "type": "is_unique"},            
                {"column": "n_regionkey", "type": "non_negative"},
            ]
        },
        "region": {
            "checks": [
                {"column": "r_regionkey", "type": "is_unique"},
            ]
        },
        "customer": {
            "checks": [
                {"column": "c_custkey", "type": "is_unique"},
                {"column": "c_custkey", "type": "non_negative"},
            ]
        },
        "lineitem": {
            "checks": [
                {"column": ["l_orderkey", "l_linenumber"], "type": "has_uniqueness"},
                {"column": "l_quantity", "type": "non_negative"},
                {"column": "l_extendedprice", "type": "non_negative"},
                {"column": "l_tax", "type": "non_negative"},
                {"column": ["l_shipdate", "l_receiptdate"], "type": "is_less_than"},
                # l_discount in range of 0 and 1
            ]
        },
        "part": {
            "checks": [
                {"column": "p_partkey", "type": "is_unique"},
                {"column": "p_partkey", "type": "non_negative"},
                {"column": "p_size", "type": "non_negative"},
                {"column": "p_retailprice", "type": "non_negative"},
            ]
        },
        "supplier": {
            "checks": [
                {"column": "s_suppkey", "type": "is_unique"},
                {"column": "s_suppkey", "type": "non_negative"},
            ]
        },
        "partsupp": {
            "checks": [
                {"column": ["ps_partkey", "ps_suppkey"], "type": "has_uniqueness"},
                {"column": "ps_partkey", "type": "non_negative"},
                {"column": "ps_availqty", "type": "non_negative"},
                {"column": "ps_supplycost", "type": "non_negative"},
            ]
        }
    }

    # data_quality_checks = create_deequ_checks(spark, table_name, data_quality_config[table_name])

    # checkResult = (
    #     VerificationSuite(spark)
    #     .onData(df)
    #     .addCheck(data_quality_checks)
    #     .run()
    # )

    # checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    # checkResult_df.show(truncate=False)

    # (
    #     checkResult_df
    #     .write
    #     .save(
    #         f"hdfs://hadoop:9000/user/hadoopuser/checks/raw_{table_name}/{dag_id}/{run_id}/check_result.json",
    #         format="json",
    #         mode="overwrite"
    #     )
    # )

    # load data
    output_path = f"hdfs://hadoop:9000/user/hadoopuser/raw/raw_{table_name}"

    # write as Delta format
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("optimizeWrite", "true")
        .save(output_path)
    )

    spark.stop()
    #NOTE: some threads are created by user code (Python) block jvm to exit
    # https://kb.databricks.com/clusters/databricks-spark-submit-jobs-appear-to-%E2%80%9Chang%E2%80%9D-and-clusters-do-not-auto-terminate-
    # long-term fix: https://issues.apache.org/jira/browse/SPARK-48547
    sc._gateway.jvm.System.exit(0)

if __name__ == "__main__":
    main()