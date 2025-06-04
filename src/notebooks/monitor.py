from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Check data quality results


def check_data_quality_results():
  try:
    quality_results = spark.read.format("delta").load("s3://banking-data-lake/monitoring/data_quality_results/")

    # Show the latest results
    latest_results = quality_results.orderBy(col("execution_date").desc()).limit(10)
    latest_results.show()

    # Show failed checks
    failed_checks = quality_results.filter(col("overall_passed") == False)
    print(f"Number of failed quality checks: {failed_checks.count()}")
    failed_checks.show()
  except Exception as e:
    print(f"Error checking data quality results: {str(e)}")

# Check pipeline execution logs


def check_pipeline_logs():
  try:
    logs = spark.read.text("s3://banking-data-lake/logs/")

    # Filter for errors
    error_logs = logs.filter(col("value").contains("ERROR"))
    print(f"Number of error logs: {error_logs.count()}")
    error_logs.show()
  except Exception as e:
    print(f"Error checking pipeline logs: {str(e)}")

# Check data counts


def check_data_counts():
  try:
    # Check customer counts
    customer_count = spark.read.format("jdbc") \
        .option("url", "jdbc:redshift://banking-warehouse.xyz.us-east-1.redshift.amazonaws.com:5439/banking") \
        .option("dbtable", "dim_customer") \
        .option("user", dbutils.secrets.get("redshift", "username")) \
        .option("password", dbutils.secrets.get("redshift", "password")) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .load() \
        .count()

    print(f"Customer count in Redshift: {customer_count}")

    # Check account counts
    account_count = spark.read.format("jdbc") \
        .option("url", "jdbc:redshift://banking-warehouse.xyz.us-east-1.redshift.amazonaws.com:5439/banking") \
        .option("dbtable", "dim_account") \
        .option("user", dbutils.secrets.get("redshift", "username")) \
        .option("password", dbutils.secrets.get("redshift", "password")) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .load() \
        .count()

    print(f"Account count in Redshift: {account_count}")

    # Check transaction counts
    transaction_count = spark.read.format("delta") \
        .load("s3://banking-data-lake/processed/transactions/") \
        .count()

    print(f"Transaction count in Delta Lake: {transaction_count}")
  except Exception as e:
    print(f"Error checking data counts: {str(e)}")


# Run the monitoring functions
print("=== Data Quality Results ===")
check_data_quality_results()

print("\n=== Pipeline Logs ===")
check_pipeline_logs()

print("\n=== Data Counts ===")
check_data_counts()
