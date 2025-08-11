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
from pyspark.sql.functions import col, sum, count


def init_spark(app_name: str = "CustomerLoyaltyTierApp"):
    """Initialize Spark Session

    Args:
        app_name (str, optional): App Name. Defaults to "CustomerLoyaltyTierApp".

    Returns:
        _type_: Spark Session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # Number of cores to run, * == Run on all cores
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

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

    df = (
        spark.read.format("csv").option("header", "true").schema(schema).load(file_path)
    )

    return df


def null_value_checker(df):
    """Checking for null values

    Args:
        df (DataFrame): DataFrame to check

    Returns:
        dict: Null value record.
    """
    null_customer_id = df.select(
        sum(col("customer_id").isNull().cast("int")).alias("Null customer_id count")
    )

    null_amount = df.select(
        sum(col("amount").isNull().cast("int")).alias("Null amount count")
    )

    null_fname = df.select(
        sum(col("first_name").isNull().cast("int")).alias("Null first_name count")
    )

    null_lname = df.select(
        sum(col("last_name").isNull().cast("int")).alias("Null last_name count")
    )
    null_email = df.select(
        sum(col("email").isNull().cast("int")).alias("Null email count")
    )

    null_reg_date = df.select(
        sum(col("registration_date").isNull().cast("int")).alias(
            "Null registration_date count"
        )
    )

    null_values = {
        "cust_id": null_customer_id,
        "amount": null_amount,
        "fname": null_fname,
        "lname": null_lname,
        "email": null_email,
        "reg_date": null_reg_date,
    }

    return null_values


def group_data():
    """Aggregate data to fetch the total transaction amount and transaction count per customer

    Returns:
        DataFrame: Aggregated data
    """
    # Create a new dataframe
    df_grouped = df.alias("df_grouped")

    df_grouped = df_grouped.groupby(col("customer_id")).agg(
        sum("amount").cast(DecimalType(10, 2)).alias("total_amount"),
        count("transaction_id").cast("int").alias("transaction_count"),
    )

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

    group_data().limit(10).show()

    spark.stop()
