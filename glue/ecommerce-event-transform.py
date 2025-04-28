from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType, StringType, DoubleType
import boto3

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client("s3")

# Define S3 Paths
RAW_PATH = "s3://ecommerce-event-data-goutham-01/raw-events/"
TRANSFORMED_PATH = "s3://ecommerce-event-data-goutham-01/transformed-events/"

# Define the expected schema
schema = StructType() \
    .add("user_id", StringType()) \
    .add("action", StringType()) \
    .add("product_id", StringType()) \
    .add("price", DoubleType())

# Read data from S3
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [RAW_PATH]},
    transformation_ctx="datasource",
)

# Convert to DataFrame
df = datasource.toDF()

# Directly parse columns (our JSON is flat, no nesting)
df_cleaned = df.select(
    col("user_id"),
    col("action"),
    col("product_id"),
    col("price"),
    current_timestamp().alias("event_time")
)

# Convert back to DynamicFrame
dynamic_dframe = DynamicFrame.fromDF(df_cleaned, glueContext, "dynamic_dframe")

# Write transformed data to S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_dframe,
    connection_type="s3",
    format="json",
    connection_options={"path": TRANSFORMED_PATH, "partitionKeys": []},
    transformation_ctx="datasink",
)
