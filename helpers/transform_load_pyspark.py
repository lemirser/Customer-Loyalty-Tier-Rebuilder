from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    DateType,
    StructField,
    StructType,
)


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


def read_file():
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


if __name__ == "__main__":
    spark = init_spark()

    print(read_file().limit(10).show())

    spark.stop()
