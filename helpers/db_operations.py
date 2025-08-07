from .logger_config import setup_logging
import logging
from .db_connection import get_database_connection
from mysql.connector import Error


setup_logging()
logger = logging.getLogger(__name__)


def select_version() -> dict:
    """Function to test the database connection

    Returns:
        dict: Database version
    """
    with get_database_connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute("SELECT VERSION();")
            logger.info("Fetched the database current version.")
            return cursor.fetchone()
        except Error as e:
            logger.error(f"Error fetching the database version: {e}")
            raise
        finally:
            cursor.close()


def get_raw_data() -> tuple:
    """Retrieve transaction data from dates 12 months prior to the current date.

    Returns:
        tuple: Result from the query.
    """

    with get_database_connection() as connection:
        cursor = connection.cursor(buffered=True)

        try:
            logger.info("Fetching transation records.")

            transaction = """
                        SELECT *
                        FROM transactions
                        WHERE
                        transaction_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
                        AND CURDATE();
                        """.strip()

            cursor.execute(transaction)

            result = cursor.fetchall()

            result_count = cursor.rowcount
            headers = cursor.column_names

            logger.info("Retrieved the transaction data.")
            logger.info(f"Number of fetched rows: {result_count}")

            return headers, result
        except Error as e:
            logger.error(f"Error fetching the transaction data: {e}")
            raise
        finally:
            cursor.close()
