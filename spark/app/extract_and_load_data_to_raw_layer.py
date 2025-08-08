import pyspark
import pydeequ
import re
import logging
import os

from delta import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *

os.environ['SPARK_VERSION'] = '3.5.5'
os.environ['HADOOP_USER_NAME'] = 'hadoopuser'

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

_logger = logging.getLogger()

builder = (
    pyspark
    .sql.SparkSession.builder
    # .appName("Manuall submission")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Deequ
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc = spark.sparkContext
print(sc.version)  # Example: Print the Spark version
print(sc.master) # Example: Print the master URL

# get arguments
# table_name = spark.conf.get("spark.app.table_name")
table_name = 'lineitem'
dag_id = spark.conf.get("spark.app.dag_id")
run_id = get_valid_dfs_name(spark.conf.get("spark.app.run_id"))

# read data
hdfs_path = f"hdfs://hadoop:9000/user/hadoopuser/landingzone/tb_{table_name}"
df = spark.read.parquet(hdfs_path)
# df = df.limit(1_000_000)
df.show(10)

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

data_quality_checks = create_deequ_checks(spark, table_name, data_quality_config[table_name])

checkResult = (
    VerificationSuite(spark)
    .onData(df)
    .addCheck(data_quality_checks)
    .run()
)

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show(truncate=False)

(
    checkResult_df
    .write
    .save(
        f"hdfs://hadoop:9000/user/hadoopuser/checks/raw_{table_name}/{dag_id}/{run_id}/check_result.json",
        format="json",
        mode="overwrite"
    )
)

# load data
output_path = f"hdfs://hadoop:9000/user/hadoopuser/raw/raw_{table_name}"

# write as delta format
# (
#     df
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .option("optimizeWrite", "true")
#     .save(output_path)
# )


# write as parquet format
# (
#     df
#     .write
#     .parquet(
#         output_path, 
#         mode="overwrite"
#     )
# )

spark.stop()

sc._gateway.jvm.System.exit(0)

# _logger.info("Spark Context stopped")
# sys.exit(0)