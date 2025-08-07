from mysql.connector import pooling, Error
from dotenv import load_dotenv
from contextlib import contextmanager
import logging
from helpers.logger_config import setup_logging
import os

# Load environment variables
load_dotenv()

# Global connection pool
_connection_pool = None

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


def initialize_database():
    """Initialize the global database connection pool."""
    global _connection_pool

    try:
        pool_config = {
            "pool_name": os.getenv("POOL_NAME", "mysql_pool_procedural"),
            "pool_size": os.getenv("POOL_SIZE", 10),
            "pool_reset_session": os.getenv("POOL_RESET_SESSION", True),
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "charset": os.getenv("CHARSET"),
            "collation": os.getenv("COLLATION"),
            "autocommit": os.getenv("AUTOCOMMIT"),
            "sql_mode": os.getenv("SQL_MODE"),
            "use_unicode": os.getenv("USE_UNICODE"),
            "connect_timeout": os.getenv("CONNECTION_TIMEOUT", 10),
            "auth_plugin": os.getenv("AUTH_PLUGIN"),
            "raise_on_warnings": True,
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
        logger.info("Connecting to database.")
        connection = _connection_pool.get_connection()
        yield connection
    except Error as e:
        if connection:
            connection.rollback()
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            logger.info("Closing the database connection.")
            connection.close()
