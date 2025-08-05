import os
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv
import logging
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_operations.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool = None

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



if __name__ == "__main__":
    print("\n=== Procedural Example ===")
    initialize_database()

    # Get DB version. Test query
    db_ver = select_version()
    print(db_ver['VERSION()'])