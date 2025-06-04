import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class TransactionTransformer:
  """Class to handle transaction data transformations."""

  def __init__(self, spark: SparkSession):
    """
    Initialize transaction transformer.

    Args:
        spark (SparkSession): Spark session
    """
    self.spark = spark

  def clean_transaction_data(self, df: DataFrame) -> DataFrame:
    """
    Clean transaction data by handling missing values and data type conversions.

    Args:
        df (DataFrame): Raw transaction data

    Returns:
        DataFrame: Cleaned transaction data
    """
    logger.info("Cleaning transaction data")

    # Convert date strings to timestamp
    df = df.withColumn("transaction_date",
                       to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))

    # Handle missing values
    df = df.na.fill("Unknown", ["merchant_name", "merchant_category", "description"])

    # Filter out invalid transactions (e.g., negative amounts for deposits)
    df = df.filter(~((col("transaction_type") == "deposit") & (col("amount") < 0)))

    # Standardize transaction types
    df = df.withColumn("transaction_type",
                       when(col("transaction_type").isin("deposit", "DEPOSIT", "Deposit"), "deposit")
                       .when(col("transaction_type").isin("withdrawal", "WITHDRAWAL", "Withdrawal"), "withdrawal")
                       .when(col("transaction_type").isin("transfer", "TRANSFER", "Transfer"), "transfer")
                       .when(col("transaction_type").isin("payment", "PAYMENT", "Payment"), "payment")
                       .otherwise(col("transaction_type")))

    return df

  def enrich_transaction_data(self, df: DataFrame) -> DataFrame:
    """
    Enrich transaction data with additional features.

    Args:
        df (DataFrame): Cleaned transaction data

    Returns:
        DataFrame: Enriched transaction data
    """
    logger.info("Enriching transaction data")

    # Extract date components
    df = df.withColumn("transaction_year", year(col("transaction_date")))
    df = df.withColumn("transaction_month", month(col("transaction_date")))
    df = df.withColumn("transaction_day", dayofmonth(col("transaction_date")))
    df = df.withColumn("transaction_hour", hour(col("transaction_date")))
    df = df.withColumn("transaction_dayofweek", dayofweek(col("transaction_date")))

    # Flag for weekend transactions
    df = df.withColumn("is_weekend",
                       when(col("transaction_dayofweek").isin(1, 7), True)
                       .otherwise(False))

    # Calculate transaction amount in USD (assuming currency conversion)
    df = df.withColumn("amount_usd",
                       when(col("currency") == "USD", col("amount"))
                       .when(col("currency") == "EUR", col("amount") * 1.1)
                       .when(col("currency") == "GBP", col("amount") * 1.3)
                       .otherwise(col("amount")))

    # Add transaction category based on merchant category
    df = df.withColumn("transaction_category",
                       when(col("merchant_category").isin("grocery", "supermarket"), "Groceries")
                       .when(col("merchant_category").isin("restaurant", "fast food"), "Dining")
                       .when(col("merchant_category").isin("gas", "fuel"), "Transportation")
                       .when(col("merchant_category").isin("utility", "electricity", "water"), "Utilities")
                       .otherwise("Other"))

    return df

  def calculate_transaction_metrics(self, df: DataFrame) -> DataFrame:
    """
    Calculate transaction metrics like running balances and spending patterns.

    Args:
        df (DataFrame): Enriched transaction data

    Returns:
        DataFrame: Transaction data with metrics
    """
    logger.info("Calculating transaction metrics")

    # Define window for running calculations by account
    window_spec = Window.partitionBy("account_id").orderBy("transaction_date")

    # Calculate running balance
    df = df.withColumn("amount_signed",
                       when(col("transaction_type").isin("deposit", "transfer_in"), col("amount_usd"))
                       .otherwise(-col("amount_usd")))

    df = df.withColumn("running_balance", sum("amount_signed").over(window_spec))

    # Calculate days since last transaction
    df = df.withColumn("prev_transaction_date",
                       lag("transaction_date", 1).over(window_spec))

    df = df.withColumn("days_since_last_transaction",
                       when(col("prev_transaction_date").isNull(), 0)
                       .otherwise(datediff(col("transaction_date"), col("prev_transaction_date"))))

    # Calculate transaction frequency metrics
    window_30d = Window.partitionBy("account_id")\
                       .orderBy("transaction_date")\
                       .rangeBetween(-30 * 86400, 0)  # 30 days in seconds

    df = df.withColumn("transaction_count_30d", count("transaction_id").over(window_30d))
    df = df.withColumn("total_spend_30d",
                       sum(when(col("transaction_type").isin("withdrawal", "payment"), col("amount_usd"))
                           .otherwise(0)).over(window_30d))

    return df

  def detect_anomalies(self, df: DataFrame) -> DataFrame:
    """
    Detect anomalous transactions based on various rules.

    Args:
        df (DataFrame): Transaction data with metrics

    Returns:
        DataFrame: Transaction data with anomaly flags
    """
    logger.info("Detecting anomalous transactions")

    # Calculate account-level statistics
    account_stats = df.groupBy("account_id").agg(
        stddev("amount_usd").alias("amount_stddev"),
        avg("amount_usd").alias("amount_avg"),
        max("amount_usd").alias("amount_max")
    )

    # Join with transaction data
    df = df.join(account_stats, on="account_id", how="left")

    # Flag large transactions (> 3 standard deviations from mean)
    df = df.withColumn("is_large_transaction",
                       (col("amount_usd") > (col("amount_avg") + 3 * col("amount_stddev"))) &
                       (col("amount_usd") > 1000))

    # Flag transactions in unusual locations
    df = df.withColumn("is_unusual_location",
                       col("is_international") &
                       ~col("location").isin("Canada", "Mexico", "United Kingdom", "France", "Germany"))

    # Flag high-frequency transactions
    df = df.withColumn("is_high_frequency",
                       col("transaction_count_30d") > 100)

    # Flag potential fraud based on combined factors
    df = df.withColumn("potential_fraud",
                       col("is_large_transaction") |
                       col("is_unusual_location") |
                       (col("days_since_last_transaction") < 0.01))  # Multiple transactions in seconds

    return df
