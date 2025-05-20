from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType
import boto3

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client("s3")

RAW_PATH = "s3://ecommerce-event-data-goutham-01/raw-events/"
TRANSFORMED_PATH = "s3://ecommerce-event-data-goutham-01/transformed-events/"

schema = StructType()\
    .add("user_id", StringType())\
    .add("action", StringType())\
    .add("product_id", StringType())\
    .add("item_name", StringType())\
    .add("quantity", StringType())\
    .add("location", StringType())\
    .add("price", DoubleType())

datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": [RAW_PATH]},
    transformation_ctx="datasource",
)

df = datasource.toDF()
df_cleaned = df.select(
    col("user_id"),
    col("action").alias("event_type"),
    col("product_id").alias("item_id"),
    col("item_name"),
    col("quantity").cast("int"),
    col("location"),
    col("price"),
    current_timestamp().alias("event_time")
)

dynamic_dframe = DynamicFrame.fromDF(df_cleaned, glueContext, "dynamic_dframe")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_dframe,
    connection_type="s3",
    format="parquet",
    connection_options={"path": TRANSFORMED_PATH, "partitionKeys": []},
    transformation_ctx="datasink",
)