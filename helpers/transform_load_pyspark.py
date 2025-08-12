from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    DateType,
    DecimalType,
    StructField,
    StructType,
)
from pyspark.sql.functions import col, sum, count, expr, lit
from logger_config import setup_logging
from dotenv import load_dotenv
import logging

# Loading .env configuration
load_dotenv()

# Logging configuration
setup_logging()
logger = logging.getLogger(__name__)


def init_spark(app_name: str = "CustomerLoyaltyTierApp"):
    """Initialize Spark Session

    Args:
        app_name (str, optional): App Name. Defaults to "CustomerLoyaltyTierApp".

    Returns:
        _type_: Spark Session
    """
    logger.info("Starting Spark Session")
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")  # Number of cores to run, * == Run on all cores
            .config("spark.sql.catalogImplementation", "in-memory")
            .getOrCreate()
        )
    except Exception as e:
        logger.error(f"Encountered an error with Spark Session: {e}")
        raise

    return spark


def read_file(spark):
    """Read the extracted file

    Returns:
        DataFrame: Spark DataFrame
    """
    schema = StructType(
        [
            StructField("transaction_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("transaction_date", DateType(), False),
            StructField("amount", DoubleType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("registration_date", DateType(), False),
        ]
    )

    file_path = "extract/upload/raw_transactions_UPLOAD.csv"
    try:
        logger.info("Loading CSV file in Spark.")
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(file_path)
        )
    except Exception as e:
        logger.error(f"Error in reading CSV file: {e}")
        raise

    return df


def null_value_checker(df):
    """Checking for null values

    Args:
        df (DataFrame): DataFrame to check

    Returns:
        dict: Null value record.
    """
    try:
        logger.info("Checking for customer_id null")
        null_customer_id = df.select(
            sum(col("customer_id").isNull().cast("int")).alias("Null customer_id count")
        )
    except Exception as e:
        logger.error(f"Error in checking customer_id: {e}")
        raise

    try:
        logger.info("Checkign for amount null")
        null_amount = df.select(
            sum(col("amount").isNull().cast("int")).alias("Null amount count")
        )
    except Exception as e:
        logger.error(f"Error in checking amount: {e}")
        raise

    try:
        logger.info("Checking first_name null")
        null_fname = df.select(
            sum(col("first_name").isNull().cast("int")).alias("Null first_name count")
        )
    except Exception as e:
        logger.error(f"Error in checking first_name: {e}")
        raise

    try:
        logger.info("Checking last_name null")
        null_lname = df.select(
            sum(col("last_name").isNull().cast("int")).alias("Null last_name count")
        )
    except Exception as e:
        logger.error(f"Error in checking last_name: {e}")
        raise

    try:
        logger.info("Checking email null")
        null_email = df.select(
            sum(col("email").isNull().cast("int")).alias("Null email count")
        )
    except Exception as e:
        logger.error(f"Error in checking email: {e}")
        raise

    try:
        logger.info("Checking registration_date null")
        null_reg_date = df.select(
            sum(col("registration_date").isNull().cast("int")).alias(
                "Null registration_date count"
            )
        )
    except Exception as e:
        logger.error(f"Error in checking registration_date: {e}")
        raise

    null_values = {
        "cust_id": null_customer_id,
        "amount": null_amount,
        "fname": null_fname,
        "lname": null_lname,
        "email": null_email,
        "reg_date": null_reg_date,
    }

    return null_values


def group_data(df):
    """Aggregate data to fetch the total transaction amount and transaction count per customer

    Returns:
        DataFrame: Aggregated data
    """
    try:
        logger.info("Grouping data")
        # Create a new dataframe
        df_grouped = df.alias("df_grouped")

        df_grouped = (
            df_grouped.groupby(col("customer_id"), col("first_name"), col("last_name"))
            .agg(
                sum("amount").cast(DecimalType(10, 2)).alias("total_amount"),
                count("transaction_id").cast("int").alias("transaction_count"),
            )
            .withColumn("email", lit("jd1388813@gmail.com"))
        )

    except Exception as e:
        logger.error(f"Error in grouping data: {e}")
        raise

    return df_grouped


def tier(df_grouped):
    """Process customer tier

    | Tier | Total Spent | Transaction Count |
    |------|-------------|-------------------|
    | **Platinum**  | ≥ ₱100,000    | ≥ 20  |
    | **Gold**      | ≥ ₱50,000     | ≥ 10  |
    | **Silver**    | ≥ ₱20,000     | ≥ 5   |
    | **Bronze**    | < ₱20,000     | < 5   |
    """

    try:
        logger.info("Processing customer tier")
        df_grouped = df_grouped.withColumn(
            "tier",
            expr(
                "CASE WHEN total_amount >= 100000 and transaction_count >= 20 THEN 'Platinum' "
                + "WHEN total_amount >= 50000 and transaction_count >= 10 THEN 'Gold' "
                + "WHEN total_amount >= 20000 and transaction_count >= 5 THEN 'Silver' "
                + "ELSE 'Bronze' END"
            ),
        )

        df_grouped = df_grouped.sort(col("customer_id").asc())
    except Exception as e:
        logger.error(f"Error in processing customer tier: {e}")
        raise

    return df_grouped


if __name__ == "__main__":
    spark = init_spark()

    df = read_file(spark)
    # print(df.limit(10).show())

    # EDA
    # df.summary().show()
    # null_value_checker(df)["cust_id"].show()
    # null_value_checker(df)["amount"].show()
    # null_value_checker(df)["fname"].show()
    # null_value_checker(df)["lname"].show()
    # null_value_checker(df)["email"].show()
    # null_value_checker(df)["reg_date"].show()

    # Tier processing
    df_grouped = group_data(df)
    df_tier = tier(df_grouped)
    df_tier.limit(10).show()

    spark.stop()
    logger.info("Stopping Spark Session")
