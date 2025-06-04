import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define schemas
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_since", DateType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("risk_segment", StringType(), True)
])

account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("open_date", DateType(), True),
    StructField("close_date", DateType(), True),
    StructField("currency", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("interest_rate", FloatType(), True),
    StructField("balance", DecimalType(18, 2), True),
    StructField("last_activity_date", DateType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("description", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("location", StringType(), True),
    StructField("is_international", BooleanType(), True)
])

# Generate customer data


def generate_customers(num_customers=1000):
  first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth"]
  last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"]
  states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
  cities = ["Los Angeles", "New York", "Houston", "Miami", "Chicago",
            "Philadelphia", "Columbus", "Atlanta", "Charlotte", "Detroit"]
  risk_segments = ["Low", "Medium", "High"]

  customers = []

  for i in range(num_customers):
    customer_id = f"CUST{i:06d}"
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)

    # Generate date of birth (21-80 years old)
    years_ago = random.randint(21, 80)
    dob = datetime.now() - timedelta(days=365 * years_ago)

    # Generate customer since date (0-10 years ago)
    years_customer = random.randint(0, 10)
    customer_since = datetime.now() - timedelta(days=365 * years_customer)

    state = random.choice(states)
    city = random.choice(cities)

    customers.append((
        customer_id,
        first_name,
        last_name,
        dob.date(),
        f"{first_name.lower()}.{last_name.lower()}@example.com",
        f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        f"{random.randint(100, 9999)} Main St",
        city,
        state,
        f"{random.randint(10000, 99999)}",
        "USA",
        customer_since.date(),
        random.randint(300, 850),
        random.choice(risk_segments)
    ))

  return spark.createDataFrame(customers, customer_schema)

# Generate account data


def generate_accounts(customers_df, num_accounts=1500):
  account_types = ["checking", "savings", "investment"]
  account_statuses = ["active", "closed", "suspended"]
  currencies = ["USD", "EUR", "GBP"]

  accounts = []

  # Get customer IDs
  customer_ids = [row.customer_id for row in customers_df.select("customer_id").collect()]

  for i in range(num_accounts):
    account_id = f"ACC{i:08d}"
    customer_id = random.choice(customer_ids)
    account_type = random.choice(account_types)
    account_status = random.choice(account_statuses)

    # Generate open date (0-5 years ago)
    years_ago = random.randint(0, 5)
    open_date = datetime.now() - timedelta(days=365 * years_ago)

    # Generate close date (only for closed accounts)
    close_date = None
    if account_status == "closed":
      days_ago = random.randint(0, 365)
      close_date = datetime.now() - timedelta(days=days_ago)

    # Generate last activity date
    days_ago = random.randint(0, 30)
    last_activity_date = datetime.now() - timedelta(days=days_ago)

    accounts.append((
        account_id,
        customer_id,
        account_type,
        account_status,
        open_date.date(),
        close_date,
        random.choice(currencies),
        f"BR{random.randint(100, 999)}",
        random.uniform(0.01, 5.0),
        random.uniform(0, 100000),
        last_activity_date.date()
    ))

  return spark.createDataFrame(accounts, account_schema)

# Generate transaction data


def generate_transactions(accounts_df, num_transactions=10000):
  transaction_types = ["deposit", "withdrawal", "transfer", "payment"]
  currencies = ["USD", "EUR", "GBP"]
  merchant_categories = ["grocery", "restaurant", "retail", "travel", "utility", "entertainment"]
  transaction_statuses = ["completed", "pending", "failed", "reversed"]
  channels = ["online", "mobile", "branch", "atm"]
  locations = ["USA", "Canada", "UK", "France", "Germany", "Japan", "Australia", "Brazil", "Mexico", "China"]

  transactions = []

  # Get active account IDs
  account_ids = [row.account_id for row in accounts_df.filter(
    col("account_status") == "active").select("account_id").collect()]

  for i in range(num_transactions):
    transaction_id = str(uuid.uuid4())
    account_id = random.choice(account_ids)

    # Generate transaction date (last 90 days)
    days_ago = random.randint(0, 90)
    hours_ago = random.randint(0, 24)
    minutes_ago = random.randint(0, 60)
    transaction_date = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)

    transaction_type = random.choice(transaction_types)

    # Amount based on transaction type
    if transaction_type == "deposit":
      amount = random.uniform(10, 5000)
    elif transaction_type == "withdrawal":
      amount = random.uniform(10, 1000)
    elif transaction_type == "transfer":
      amount = random.uniform(10, 3000)
    else:  # payment
      amount = random.uniform(10, 2000)

    currency = random.choice(currencies)
    merchant_category = random.choice(merchant_categories)

    # Generate merchant name based on category
    if merchant_category == "grocery":
      merchant_name = random.choice(["Whole Foods", "Safeway", "Kroger", "Trader Joe's"])
    elif merchant_category == "restaurant":
      merchant_name = random.choice(["McDonald's", "Starbucks", "Chipotle", "Olive Garden"])
    elif merchant_category == "retail":
      merchant_name = random.choice(["Amazon", "Walmart", "Target", "Best Buy"])
    elif merchant_category == "travel":
      merchant_name = random.choice(["Delta Airlines", "Marriott", "Expedia", "Uber"])
    elif merchant_category == "utility":
      merchant_name = random.choice(["AT&T", "PG&E", "Comcast", "Verizon"])
    else:  # entertainment
      merchant_name = random.choice(["Netflix", "AMC Theaters", "Spotify", "Disney+"])

    location = random.choice(locations)
    is_international = location != "USA"

    transactions.append((
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        amount,
        currency,
        f"{transaction_type.capitalize()} at {merchant_name}",
        merchant_name,
        merchant_category,
        random.choice(transaction_statuses),
        random.choice(channels),
        location,
        is_international
    ))

  return spark.createDataFrame(transactions, transaction_schema)


# Generate the data
customers_df = generate_customers(1000)
accounts_df = generate_accounts(customers_df, 1500)
transactions_df = generate_transactions(accounts_df, 10000)

# Write data to S3
customers_df.write.mode("overwrite").csv("s3://banking-data-lake/raw/customers/", header=True)
accounts_df.write.mode("overwrite").csv("s3://banking-data-lake/raw/accounts/", header=True)
transactions_df.write.mode("overwrite").csv("s3://banking-data-lake/raw/transactions/", header=True)

print("Sample data generation complete!")
