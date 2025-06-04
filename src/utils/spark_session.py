import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_spark_session(app_name="Banking ETL Pipeline"):
  """
  Create and configure a Spark session for the ETL pipeline.

  Args:
      app_name (str): Name of the Spark application

  Returns:
      SparkSession: Configured Spark session
  """
  return (SparkSession.builder
          .appName(app_name)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
          .config("spark.sql.warehouse.dir", "s3://your-data-lake-bucket/warehouse")
          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                  "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
          .config("spark.hadoop.fs.s3a.connection.maximum", "100")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.shuffle.partitions", "4")  # Reduced for local development
          .config("spark.default.parallelism", "4")     # Reduced for local development
          .enableHiveSupport()
          .getOrCreate())
