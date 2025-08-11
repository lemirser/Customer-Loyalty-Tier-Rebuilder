from pyspark.sql import SparkSession


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


if __name__ == "__main__":
    spark = init_spark()

    print("Spark Session")

    spark.stop()
