import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession

# from src.ingestion.rds_connector import RDSConnector
from src.ingestion.s3_connector import S3Connector
from src.loading.redshift_loader import RedshiftLoader
# from src.loading.s3_loader import S3Loader
# from src.transformation.account_transform import AccountTransformer
# from src.transformation.customer_transform import CustomerTransformer
from src.transformation.data_quality import DataQualityChecker
from src.transformation.transaction_transform import TransactionTransformer
from src.utils.logging_utils import setup_logging
# Import project modules
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)


class BankingETLPipeline:
  """Main class to orchestrate the banking ETL pipeline."""

  def __init__(self, config_path: str):
    """
    Initialize the ETL pipeline.

    Args:
        config_path (str): Path to the configuration file
    """
    # Set up logging
    setup_logging()

    # Load configuration
    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r') as config_file:
      self.config = json.load(config_file)

    # Initialize Spark session
    logger.info("Initializing Spark session")
    self.spark = create_spark_session(app_name=self.config.get("app_name", "Banking ETL Pipeline"))

    # Initialize components
    self._init_components()

    # Set execution date
    self.execution_date = datetime.now().strftime("%Y-%m-%d")

  def _init_components(self):
    """Initialize pipeline components based on configuration."""
    logger.info("Initializing pipeline components")

    # Initialize data connectors
    s3_config = self.config.get("s3", {})
    self.s3_connector = S3Connector(
        self.spark,
        s3_config.get("bucket_name", "banking-data-lake")
    )

    # rds_config = self.config.get("rds", {})
    # self.rds_connector = RDSConnector(
    #     self.spark,
    #     rds_config.get("jdbc_url"),
    #     rds_config.get("username"),
    #     rds_config.get("password")
    # )

    # Initialize transformers
    # self.customer_transformer = CustomerTransformer(self.spark)
    self.transaction_transformer = TransactionTransformer(self.spark)
    # self.account_transformer = AccountTransformer(self.spark)

    # Initialize data quality checker
    self.data_quality_checker = DataQualityChecker(self.spark)

    # Initialize data loaders
    redshift_config = self.config.get("redshift", {})
    self.redshift_loader = RedshiftLoader(
        self.spark,
        redshift_config.get("jdbc_url"),
        redshift_config.get("username"),
        redshift_config.get("password")
    )

    # self.s3_loader = S3Loader(
    #     self.spark,
    #     s3_config.get("bucket_name", "banking-data-lake")
    # )

  def run_customer_pipeline(self):
    """Run the customer data pipeline."""
    logger.info("Running customer data pipeline")

    try:
      # Extract customer data
      customer_config = self.config.get("pipelines", {}).get("customer", {})
      source_type = customer_config.get("source_type")

      if source_type == "s3":
        raw_customers = self.s3_connector.read_csv(
            customer_config.get("source_path")
        )
      elif source_type == "rds":
        raw_customers = self.rds_connector.read_table(
            customer_config.get("source_table")
        )
      else:
        raise ValueError(f"Unsupported source type: {source_type}")

      # Transform customer data
      cleaned_customers = self.customer_transformer.clean_customer_data(raw_customers)
      enriched_customers = self.customer_transformer.enrich_customer_data(cleaned_customers)

      # Run data quality checks
      quality_results = self.data_quality_checker.run_all_checks(
          enriched_customers,
          customer_config.get("data_quality", {})
      )

      if not quality_results.get("overall_passed", False):
        logger.warning("Data quality checks failed for customer data")
        # Depending on configuration, we might still proceed
        if customer_config.get("fail_on_quality_check", True):
          raise Exception("Data quality checks failed for customer data")

      # Load customer data
      target_type = customer_config.get("target_type")

      if target_type == "redshift":
        self.redshift_loader.load_with_staging(
            enriched_customers,
            customer_config.get("target_table"),
            key_columns=customer_config.get("key_columns", ["customer_id"])
        )
      elif target_type == "s3":
        self.s3_loader.write_delta(
            enriched_customers,
            customer_config.get("target_path"),
            mode=customer_config.get("write_mode", "overwrite"),
            partition_cols=customer_config.get("partition_cols", [])
        )
      else:
        raise ValueError(f"Unsupported target type: {target_type}")

      logger.info("Customer data pipeline completed successfully")
      return True
    except Exception as e:
      logger.error(f"Error in customer data pipeline: {str(e)}")
      raise

  def run_transaction_pipeline(self):
    """Run the transaction data pipeline."""
    logger.info("Running transaction data pipeline")

    try:
      # Extract transaction data
      transaction_config = self.config.get("pipelines", {}).get("transaction", {})
      source_type = transaction_config.get("source_type")

      if source_type == "s3":
        raw_transactions = self.s3_connector.read_csv(
            transaction_config.get("source_path")
        )
      elif source_type == "rds":
        raw_transactions = self.rds_connector.read_table(
            transaction_config.get("source_table")
        )
      else:
        raise ValueError(f"Unsupported source type: {source_type}")

      # Transform transaction data
      cleaned_transactions = self.transaction_transformer.clean_transaction_data(raw_transactions)
      enriched_transactions = self.transaction_transformer.enrich_transaction_data(cleaned_transactions)
      transactions_with_metrics = self.transaction_transformer.calculate_transaction_metrics(enriched_transactions)
      final_transactions = self.transaction_transformer.detect_anomalies(transactions_with_metrics)

      # Run data quality checks
      quality_results = self.data_quality_checker.run_all_checks(
          final_transactions,
          transaction_config.get("data_quality", {})
      )

      if not quality_results.get("overall_passed", False):
        logger.warning("Data quality checks failed for transaction data")
        # Depending on configuration, we might still proceed
        if transaction_config.get("fail_on_quality_check", True):
          raise Exception("Data quality checks failed for transaction data")

      # Load transaction data
      target_type = transaction_config.get("target_type")

      if target_type == "redshift":
        self.redshift_loader.load_with_staging(
            final_transactions,
            transaction_config.get("target_table"),
            key_columns=transaction_config.get("key_columns", ["transaction_id"])
        )
      elif target_type == "s3":
        self.s3_loader.write_delta(
            final_transactions,
            transaction_config.get("target_path"),
            mode=transaction_config.get("write_mode", "append"),
            partition_cols=transaction_config.get("partition_cols", ["transaction_year", "transaction_month"])
        )
      else:
        raise ValueError(f"Unsupported target type: {target_type}")

      logger.info("Transaction data pipeline completed successfully")
      return True
    except Exception as e:
      logger.error(f"Error in transaction data pipeline: {str(e)}")
      raise

  def run_account_pipeline(self):
    """Run the account data pipeline."""
    logger.info("Running account data pipeline")

    try:
      # Extract account data
      account_config = self.config.get("pipelines", {}).get("account", {})
      source_type = account_config.get("source_type")

      if source_type == "s3":
        raw_accounts = self.s3_connector.read_csv(
            account_config.get("source_path")
        )
      elif source_type == "rds":
        raw_accounts = self.rds_connector.read_table(
            account_config.get("source_table")
        )
      else:
        raise ValueError(f"Unsupported source type: {source_type}")

      # Transform account data
      cleaned_accounts = self.account_transformer.clean_account_data(raw_accounts)
      enriched_accounts = self.account_transformer.enrich_account_data(cleaned_accounts)

      # Run data quality checks
      quality_results = self.data_quality_checker.run_all_checks(
          enriched_accounts,
          account_config.get("data_quality", {})
      )

      if not quality_results.get("overall_passed", False):
        logger.warning("Data quality checks failed for account data")
        # Depending on configuration, we might still proceed
        if account_config.get("fail_on_quality_check", True):
          raise Exception("Data quality checks failed for account data")

      # Load account data
      target_type = account_config.get("target_type")

      if target_type == "redshift":
        self.redshift_loader.load_with_staging(
            enriched_accounts,
            account_config.get("target_table"),
            key_columns=account_config.get("key_columns", ["account_id"])
        )
      elif target_type == "s3":
        self.s3_loader.write_delta(
            enriched_accounts,
            account_config.get("target_path"),
            mode=account_config.get("write_mode", "overwrite"),
            partition_cols=account_config.get("partition_cols", [])
        )
      else:
        raise ValueError(f"Unsupported target type: {target_type}")

      logger.info("Account data pipeline completed successfully")
      return True
    except Exception as e:
      logger.error(f"Error in account data pipeline: {str(e)}")
      raise

  def run_pipeline(self):
    """Run the complete ETL pipeline."""
    logger.info("Starting the banking ETL pipeline")

    try:
      # Run individual pipelines based on configuration
      pipelines_to_run = self.config.get("pipelines_to_run", [])

      if "customer" in pipelines_to_run:
        self.run_customer_pipeline()

      if "account" in pipelines_to_run:
        self.run_account_pipeline()

      if "transaction" in pipelines_to_run:
        self.run_transaction_pipeline()

      logger.info("Banking ETL pipeline completed successfully")
      return True
    except Exception as e:
      logger.error(f"Error in banking ETL pipeline: {str(e)}")
      raise
    finally:
      # Clean up resources
      logger.info("Cleaning up resources")
      self.spark.stop()


if __name__ == "__main__":
  # Get configuration path from environment variable or use default
  config_path = os.environ.get("ETL_CONFIG_PATH", "config/config.json")

  # Run the pipeline
  pipeline = BankingETLPipeline(config_path)
  pipeline.run_pipeline()
