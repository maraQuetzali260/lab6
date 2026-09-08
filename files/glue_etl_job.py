"""
glue_etl_job.py
----------------
AWS Glue (PySpark) ETL job that cleanses the alumnus dataset
(name, age, date_of_joining, level, mail_address, living_address).

Pipeline shape:

    Glue Data Catalog (source table)
              |
              v
        [ Extract ]  -> DynamicFrame -> Spark DataFrame
              |
              v
        [ Transform ] -> trim free-text fields
                       -> apply business-rule validators as UDFs
                       -> split into valid / invalid record sets
              |
              v
        [ Load ]  -> valid   -> curated S3 path   (Parquet)
                  -> invalid -> quarantine S3 path (Parquet, tagged with
                                the reason(s) it was rejected)

Deploy this file to S3 alongside cleansing_rules.py (same prefix) and set
it as the Glue job's script location + a "--extra-py-files" job parameter
pointing at cleansing_rules.py (see README.md for full deployment steps).
"""

import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from cleansing_rules import (
    is_valid_age,
    is_valid_date_format,
    is_valid_level,
    clean_string_field,
)

# ---------------------------------------------------------------------------
# Job bootstrap
# ---------------------------------------------------------------------------
REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_DATABASE",     # Glue Data Catalog database of the source table
    "SOURCE_TABLE",        # Glue Data Catalog table of the source alumnus data
    "OUTPUT_PATH",         # e.g. s3://my-bucket/curated/alumnus/
    "QUARANTINE_PATH",     # e.g. s3://my-bucket/quarantine/alumnus/
]

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name=args["SOURCE_TABLE"],
    transformation_ctx="source_dyf",
)

df = source_dyf.toDF()

expected_columns = {
    "name",
    "age",
    "date_of_joining",
    "level",
    "mail_address",
    "living_address",
}
missing_columns = expected_columns - set(df.columns)
if missing_columns:
    raise ValueError(f"Source table is missing expected column(s): {sorted(missing_columns)}")

# ---------------------------------------------------------------------------
# Register cleansing functions as Spark UDFs
# ---------------------------------------------------------------------------
age_udf = F.udf(is_valid_age, T.BooleanType())
date_udf = F.udf(is_valid_date_format, T.BooleanType())
level_udf = F.udf(is_valid_level, T.BooleanType())
clean_str_udf = F.udf(clean_string_field, T.StringType())


def _build_rejection_reason(valid_age, valid_date, valid_level, valid_name):
    reasons = []
    if not valid_age:
        reasons.append("INVALID_AGE")
    if not valid_date:
        reasons.append("INVALID_DATE_FORMAT")
    if not valid_level:
        reasons.append("INVALID_LEVEL")
    if not valid_name:
        reasons.append("MISSING_NAME")
    return ",".join(reasons) if reasons else None


reason_udf = F.udf(_build_rejection_reason, T.StringType())

# ---------------------------------------------------------------------------
# Transform: trim / normalize free-text fields (does NOT alter age/date/level
# values themselves -- those are validated as-is per the business rules)
# ---------------------------------------------------------------------------
df_clean = (
    df.withColumn("name", clean_str_udf(F.col("name")))
    .withColumn("mail_address", clean_str_udf(F.col("mail_address")))
    .withColumn("living_address", clean_str_udf(F.col("living_address")))
)

# ---------------------------------------------------------------------------
# Transform: flag each record against the business rules
# ---------------------------------------------------------------------------
df_flagged = (
    df_clean.withColumn("_valid_age", age_udf(F.col("age")))
    .withColumn("_valid_date", date_udf(F.col("date_of_joining")))
    .withColumn("_valid_level", level_udf(F.col("level")))
    .withColumn("_valid_name", F.col("name").isNotNull())
)

df_flagged = df_flagged.withColumn(
    "_is_valid",
    F.col("_valid_age") & F.col("_valid_date") & F.col("_valid_level") & F.col("_valid_name"),
).withColumn(
    "_rejection_reason",
    reason_udf(
        F.col("_valid_age"), F.col("_valid_date"), F.col("_valid_level"), F.col("_valid_name")
    ),
)

# ---------------------------------------------------------------------------
# Split valid / invalid record sets
# ---------------------------------------------------------------------------
helper_cols = ["_valid_age", "_valid_date", "_valid_level", "_valid_name"]

valid_df = df_flagged.filter(F.col("_is_valid")).drop(
    *helper_cols, "_is_valid", "_rejection_reason"
)

invalid_df = df_flagged.filter(~F.col("_is_valid")).drop(*helper_cols, "_is_valid")

valid_count = valid_df.count()
invalid_count = invalid_df.count()
print(f"[alumnus-etl] valid records: {valid_count} | invalid records: {invalid_count}")

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
valid_dyf = DynamicFrame.fromDF(valid_df, glueContext, "valid_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=valid_dyf,
    connection_type="s3",
    connection_options={"path": args["OUTPUT_PATH"], "partitionKeys": []},
    format="parquet",
    transformation_ctx="write_valid",
)

if invalid_count > 0:
    invalid_dyf = DynamicFrame.fromDF(invalid_df, glueContext, "invalid_dyf")
    glueContext.write_dynamic_frame.from_options(
        frame=invalid_dyf,
        connection_type="s3",
        connection_options={"path": args["QUARANTINE_PATH"], "partitionKeys": []},
        format="parquet",
        transformation_ctx="write_invalid",
    )

job.commit()
