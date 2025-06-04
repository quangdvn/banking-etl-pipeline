import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)


class S3Connector:
  """Class to handle data ingestion from AWS S3."""

  def __init__(self, spark: SparkSession, bucket_name: str):
    """
    Initialize S3 connector.

    Args:
        spark (SparkSession): Spark session
        bucket_name (str): S3 bucket name
    """
    self.spark = spark
    self.bucket_name = bucket_name

  def read_csv(self, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
    """
    Read CSV file from S3.

    Args:
        file_path (str): Path to the CSV file in S3
        header (bool): Whether the CSV has a header
        infer_schema (bool): Whether to infer the schema

    Returns:
        DataFrame: Spark DataFrame containing the data
    """
    try:
      full_path = f"s3a://{self.bucket_name}/{file_path}"
      logger.info(f"Reading CSV file from {full_path}")

      return (self.spark.read
              .option("header", header)
              .option("inferSchema", infer_schema)
              .csv(full_path))
    except Exception as e:
      logger.error(f"Error reading CSV file from {full_path}: {str(e)}")
      raise

  def read_parquet(self, file_path: str) -> DataFrame:
    """
    Read Parquet file from S3.

    Args:
        file_path (str): Path to the Parquet file in S3

    Returns:
        DataFrame: Spark DataFrame containing the data
    """
    try:
      full_path = f"s3a://{self.bucket_name}/{file_path}"
      logger.info(f"Reading Parquet file from {full_path}")

      return self.spark.read.parquet(full_path)
    except Exception as e:
      logger.error(f"Error reading Parquet file from {full_path}: {str(e)}")
      raise

  def read_delta(self, file_path: str) -> DataFrame:
    """
    Read Delta table from S3.

    Args:
        file_path (str): Path to the Delta table in S3

    Returns:
        DataFrame: Spark DataFrame containing the data
    """
    try:
      full_path = f"s3a://{self.bucket_name}/{file_path}"
      logger.info(f"Reading Delta table from {full_path}")

      return self.spark.read.format("delta").load(full_path)
    except Exception as e:
      logger.error(f"Error reading Delta table from {full_path}: {str(e)}")
      raise
