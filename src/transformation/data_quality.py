import logging
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)


class DataQualityChecker:
  """Class to perform data quality checks on datasets."""

  def __init__(self, spark: SparkSession):
    """
    Initialize data quality checker.

    Args:
        spark (SparkSession): Spark session
    """
    self.spark = spark

  def check_nulls(self, df: DataFrame, required_columns: List[str]) -> Tuple[bool, Dict[str, int]]:
    """
    Check for null values in required columns.

    Args:
        df (DataFrame): DataFrame to check
        required_columns (List[str]): List of columns that should not have nulls

    Returns:
        Tuple[bool, Dict[str, int]]: (passed/failed, dict of null counts by column)
    """
    logger.info(f"Checking for nulls in columns: {required_columns}")

    # Count nulls in each column
    null_counts = {}
    for column in required_columns:
      if column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count
      else:
        logger.warning(f"Column {column} not found in DataFrame")
        null_counts[column] = "Column not found"

    # Check if any required column has nulls
    has_nulls = any(isinstance(count, int) and count > 0 for count in null_counts.values())

    if has_nulls:
      logger.warning(f"Null check failed. Null counts: {null_counts}")
      return False, null_counts

    logger.info("Null check passed")
    return True, null_counts

  def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Tuple[bool, int]:
    """
    Check for duplicate records based on key columns.

    Args:
        df (DataFrame): DataFrame to check
        key_columns (List[str]): Columns that should form a unique key

    Returns:
        Tuple[bool, int]: (passed/failed, count of duplicate records)
    """
    logger.info(f"Checking for duplicates on key columns: {key_columns}")

    # Count total rows
    total_rows = df.count()

    # Count distinct rows based on key columns
    distinct_rows = df.select(key_columns).distinct().count()

    # Calculate duplicates
    duplicate_count = total_rows - distinct_rows

    if duplicate_count > 0:
      logger.warning(f"Duplicate check failed. Found {duplicate_count} duplicates")
      return False, duplicate_count
    logger.info("Duplicate check passed")
    return True, 0

  def check_data_ranges(self, df: DataFrame, range_checks: Dict[str, Tuple]) -> Tuple[bool, Dict[str, int]]:
    """
    Check if values in columns fall within expected ranges.

    Args:
        df (DataFrame): DataFrame to check
        range_checks (Dict[str, Tuple]): Dictionary mapping column names to (min, max) tuples

    Returns:
        Tuple[bool, Dict[str, int]]: (passed/failed, dict of out-of-range counts by column)
    """
    logger.info(f"Checking data ranges for columns: {list(range_checks.keys())}")

    out_of_range_counts = {}

    for column, (min_val, max_val) in range_checks.items():
      if column in df.columns:
        # Count values outside the expected range
        out_of_range_count = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()

        out_of_range_counts[column] = out_of_range_count
      else:
        logger.warning(f"Column {column} not found in DataFrame")
        out_of_range_counts[column] = "Column not found"

    # Check if any column has out-of-range values
    has_out_of_range = any(isinstance(count, int) and count > 0 for count in out_of_range_counts.values())

    if has_out_of_range:
      logger.warning(f"Range check failed. Out-of-range counts: {out_of_range_counts}")
      return False, out_of_range_counts
    else:
      logger.info("Range check passed")
      return True, out_of_range_counts

  def check_referential_integrity(self, df: DataFrame, ref_df: DataFrame,
                                  fk_column: str, pk_column: str) -> Tuple[bool, int]:
    """
    Check referential integrity between two DataFrames.

    Args:
        df (DataFrame): DataFrame with foreign key
        ref_df (DataFrame): Reference DataFrame with primary key
        fk_column (str): Foreign key column in df
        pk_column (str): Primary key column in ref_df

    Returns:
        Tuple[bool, int]: (passed/failed, count of orphaned records)
    """
    logger.info(f"Checking referential integrity: {fk_column} -> {pk_column}")

    # Get distinct foreign keys
    fk_values = df.select(fk_column).distinct()

    # Get distinct primary keys
    pk_values = ref_df.select(pk_column).distinct()

    # Find orphaned records (foreign keys without matching primary keys)
    orphaned_records = fk_values.join(
        pk_values,
        fk_values[fk_column] == pk_values[pk_column],
        "left_anti"
    )

    orphaned_count = orphaned_records.count()

    if orphaned_count > 0:
      logger.warning(f"Referential integrity check failed. Found {orphaned_count} orphaned records")
      return False, orphaned_count
    else:
      logger.info("Referential integrity check passed")
      return True, 0

  def run_all_checks(self, df: DataFrame, check_config: Dict) -> Dict:
    """
    Run all configured data quality checks on a DataFrame.

    Args:
        df (DataFrame): DataFrame to check
        check_config (Dict): Configuration for checks to run

    Returns:
        Dict: Results of all checks
    """
    logger.info(f"Running all data quality checks for table: {check_config.get('table_name', 'unknown')}")

    results = {
        "table_name": check_config.get("table_name", "unknown"),
        "record_count": df.count(),
        "checks": {}
    }

    # Run null checks
    if "required_columns" in check_config:
      null_check_passed, null_counts = self.check_nulls(df, check_config["required_columns"])
      results["checks"]["null_check"] = {
          "passed": null_check_passed,
          "details": null_counts
      }

    # Run duplicate checks
    if "key_columns" in check_config:
      dup_check_passed, dup_count = self.check_duplicates(df, check_config["key_columns"])
      results["checks"]["duplicate_check"] = {
          "passed": dup_check_passed,
          "details": {"duplicate_count": dup_count}
      }

    # Run range checks
    if "range_checks" in check_config:
      range_check_passed, range_counts = self.check_data_ranges(df, check_config["range_checks"])
      results["checks"]["range_check"] = {
          "passed": range_check_passed,
          "details": range_counts
      }

    # Overall check result
    results["overall_passed"] = all(check["passed"] for check in results["checks"].values())

    return results
