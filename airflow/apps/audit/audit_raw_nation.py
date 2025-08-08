import pyspark
import pydeequ
import re

from delta import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *
from pprint import pprint
from pyspark.sql.functions import *


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
        elif check_type == "is_complete":
            check = check.isComplete(column)

    return check

def main():

    builder = (
        pyspark
        .sql.SparkSession.builder
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

    dag_id = spark.conf.get("spark.app.dag_id")
    run_id = spark.conf.get("spark.app.run_id")

    # read data
    hdfs_path = f"hdfs://hadoop:9000/user/hadoopuser/raw/raw_nation"
    df = spark.read.format("delta").load(hdfs_path)

    # data quality checks
    data_quality_config = {
        "raw_nation": {
            "checks": [                               
                {'column': 'n_nationkey', "type": 'is_complete'},
                {'column': 'n_nationkey', "type": 'is_unique'},
            ]
        },
    }

    table_name = "raw_nation"
    data_quality_checks = create_deequ_checks(spark, table_name, data_quality_config[table_name])

    checkResult = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(data_quality_checks)
        .run()
    )

    checkResult_df = (
        VerificationResult.checkResultsAsDataFrame(spark, checkResult)
        .withColumn("dag_id", lit(dag_id))
        .withColumn("run_id", lit(run_id))
        .withColumn("check_date", current_timestamp())
    )
    checkResult_df.show(truncate=False)
    (
        checkResult_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"hdfs://hadoop:9000/user/hadoopuser/audit/{table_name}")
    )

    failed_checks_df = (
        checkResult_df
        .filter(col("constraint_status") == "Failure")
    )
    
    if failed_checks_df.count() > 0:
        raise Exception("Data quality checks failed!")

    spark.stop()
    #NOTE: some threads are created by user code (Python) block jvm to exit
    # https://kb.databricks.com/clusters/databricks-spark-submit-jobs-appear-to-%E2%80%9Chang%E2%80%9D-and-clusters-do-not-auto-terminate-
    # long-term fix: https://issues.apache.org/jira/browse/SPARK-48547
    sc._gateway.jvm.System.exit(0)

if __name__ == "__main__":
    main()