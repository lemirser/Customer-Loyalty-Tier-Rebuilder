import csv
from datetime import datetime
import shutil
from .logger_config import setup_logging
import logging
from .db_operations import get_raw_data

# from mysql.connector import Error
import os

setup_logging()
logger = logging.getLogger(__name__)

# Reference for file names
time_ref = datetime.now()
_time_ref = time_ref.strftime("%Y%m%d_%H%M%S")  # Add uniqueness to the file name
dest_path_backup = "extract/archive/"
dest_path_csv = "extract/"
upload_path_csv = "extract/upload/"


def raw_to_csv() -> str:
    """Store the transaction data into a CSV file for ingestion in Databricks.

    Returns:
        str: Filename.
    """

    try:
        result = get_raw_data()

        file_name = "raw_transaction.csv"

        with open(f"{dest_path_csv}{file_name}", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(result[0])  # Write column names
            writer.writerows(result[1])  # Write data

        logger.info("Creating CSV file from the query result.")
    except (OSError, csv.Error, ValueError) as e:
        logger.error(f"Error in creating a CSV file: {e}")
        raise

    return file_name


def backup_file(filename: str) -> bool:
    """Move the extracted CSV to the appropriate folder location.

    Args:
        filename (str): Orignal CSV extracted from raw_to_csv()

    Returns:
        bool: Returns True if the process did not encountered any issue.
    """

    check_file = f"{dest_path_csv}{filename}"
    try:
        if os.path.exists(check_file):
            logger.info("Raw file exists.")
        else:
            logger.error("Raw file does not exists.")
            raise

        # Copy Original file to Archive folder
        shutil.copy(
            f"{dest_path_csv}{filename}", dest_path_backup
        )  # (source_file, destination_path)
        logger.info("Archiving the original file.")

        # Rename the archived file
        shutil.move(
            f"{dest_path_backup}{filename}",
            f"{dest_path_backup}raw_transactions_{_time_ref}.csv",
        )
        logger.info("Renaming archived file.")

        # Move and rename file for upload to Databricks
        shutil.move(
            f"{dest_path_csv}{filename}",
            f"{upload_path_csv}raw_transactions_UPLOAD.csv",
        )
        logger.info("Moving original file to upload folder.")

    except PermissionError:
        logger.error(f"Permission denied copying to: {upload_path_csv}")
        raise
    except shutil.Error as e:
        logger.error(f"Shutil error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

    return True
