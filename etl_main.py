from dotenv import load_dotenv
import logging
from helpers.logger_config import setup_logging
from helpers.file_uploader import file_uploader
from helpers.db_connection import initialize_database
from helpers.etl_utils import raw_to_csv, backup_file
from helpers.transform_load_pyspark import (
    init_spark,
    read_file,
    group_data,
    tier,
    process_user_email,
)
from helpers.email_notifier import html_template, send_email
import os

# Load environment variables
load_dotenv()

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("=== Process start ===")
    initialize_database()

    # Get DB version. Test query
    # db_ver = select_version()
    # print(db_ver)

    # Fetch raw data
    # raw_data = get_raw_data()
    # print(raw_data)

    # Extract CSV
    # raw_file = raw_to_csv()
    # backup_file(raw_file)

    # File uploader
    # file_uploader()

    # Spark Session
    spark = init_spark()
    df = read_file(spark)
    df_grouped = group_data(df)
    df_tier = tier(df_grouped)
    user_details = process_user_email(df_tier)

    # Emailing
    for user in user_details.items():
        main_template = html_template(
            current_tier=user[1]["tier"],
            old_tier="Platinum",  # Testing
            customer_name=f"{user[1]["fname"]} {user[1]["lname"]}",
            customer_email=user[0],
            sum_amount=user[1]["total_amount"],
            transaction_count=user[1]["transaction_count"],
        )

        send_email(
            sender_email=os.getenv("EMAIL_USER"),
            sender_password=os.getenv("EMAIL_PASS"),
            receiver_emails=user[0],
            html_content=main_template,
        )

    logger.info("=== Process end ===")
