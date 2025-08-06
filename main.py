import os
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv
import logging
from contextlib import contextmanager
import csv
from datetime import datetime
import shutil
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool = None

# Reference for file names
time_ref = datetime.now()
_time_ref = time_ref.strftime('%Y%m%d_%H%M%S') # Add uniqueness to the file name
dest_path_backup = "extract/archive/"
dest_path_csv = "extract/"
upload_path_csv = "extract/upload/"


def initialize_database():
    """Initialize the global database connection pool."""
    global _connection_pool

    try:
        pool_config = {
            'pool_name': 'mysql_pool_procedural',
            'pool_size': 10,
            'pool_reset_session': True,
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'autocommit': False,
            'sql_mode': 'STRICT_TRANS_TABLES',
            'use_unicode': True,
            'connect_timeout': 10,
            'raise_on_warnings': True
        }

        _connection_pool = pooling.MySQLConnectionPool(**pool_config)
        logger.info("Database initialized successfully")
        return True

    except Error as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

@contextmanager
def get_database_connection():
    """Get a database connection from the pool with automatic cleanup."""

    if not _connection_pool:
        logger.error("Database not initialized.")
        raise Exception("Database not initialized. Call initialize_database() first.")

    connection = None
    try:
        connection = _connection_pool.get_connection()
        yield connection
    except Error as e:
        if connection:
            connection.rollback()
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()

def select_version():
    with get_database_connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute("SELECT VERSION();")
            return cursor.fetchone()
        except Error as e:
            logger.error(f"Error during executing the query: {e}")
            raise
        finally:
            cursor.close()

def get_raw_data() -> tuple:
    """Retrieve transaction data from dates 12 months prior to the current date.

    Returns:
        tuple: Result from the query.
    """

    with get_database_connection() as connection:
        cursor = connection.cursor( buffered=True)

        try:
            logging.info("Fetching transation records.")

            transaction = "SELECT * FROM transactions WHERE transaction_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE() LIMIT 10;"

            cursor.execute(transaction)

            result = cursor.fetchall()

            headers = cursor.column_names

            return headers, result
        except Error as e:
            logger.error(f"Error during executing the query: {e}")
            raise
        finally:
            cursor.close()

def raw_to_csv() -> str:
    """Store the transaction data into CSV file for ingestion in Databricks."""

    try:
        result = get_raw_data()

        with open(f"{dest_path_csv}raw_transaction.csv","w", newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(result[0]) # Write column names
            writer.writerows(result[1]) # Write data

        logger.info("Creating CSV file from the query result.")
    except Error as e:
        logger.error(f"Error was encountered: {e}")
        raise

    return f"raw_transaction.csv"

def backup_file(filename:str) -> bool:
    """Move extracted CSV to the appropriate folder location.

    Args:
        filename (str): Orignal CSV extracted from raw_to_csv()

    Returns:
        bool: Returns True if the process did not encountered any issue.
    """

    check_file = f"{dest_path_csv}{filename}"
    try:
        if os.path.exists(check_file):
            logging.info("Raw file exists.")
        else:
            logging.error("Raw file does not exists.")
            raise

        # Copy Original file to Archive folder
        shutil.copy(f"{dest_path_csv}{filename}", dest_path_backup) #(source_file, destination_path)
        logging.info("Archiving original files")

        # Rename the archived file
        shutil.move(f"{dest_path_backup}{filename}", f"{dest_path_backup}raw_transactions_{_time_ref}.csv")
        logging.info("Renaming archived file.")

        # Move and rename file for upload to Databricks
        shutil.move(f"{dest_path_csv}{filename}", f"{upload_path_csv}raw_transactions_UPLOAD.csv")
        logging.info("Moving original file in upload folder.")
    except Error as e:
        logging.error(f"An error was encountered: {e}")
        raise

    return True



if __name__ == "__main__":
    logging.info("=== Process start ===")
    initialize_database()

    # Get DB version. Test query
    # db_ver = select_version()

    # Extract CSV
    raw_file = raw_to_csv()
    backup_file(raw_file)
    logging.info("=== Process end ===")
    # print(raw_file)