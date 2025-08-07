from dotenv import load_dotenv
import logging
from helpers.logger_config import setup_logging
from helpers.file_uploader import file_uploader
from helpers.db_connection import initialize_database
from helpers.etl_utils import raw_to_csv, backup_file

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
    raw_file = raw_to_csv()
    backup_file(raw_file)

    # File uploader
    file_uploader()

    logger.info("=== Process end ===")
