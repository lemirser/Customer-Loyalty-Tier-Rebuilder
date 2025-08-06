import io
from databricks.sdk import WorkspaceClient
from datetime import datetime
from dotenv import load_dotenv
from .logger_config import setup_logging
import logging
from pathlib import Path

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)

# Initialize the WorkspaceClient
w = WorkspaceClient()
logger.info("Starting Databricks setup.")

time_ref = datetime.now()
_time_ref = time_ref.strftime("%Y%m%d_%H%M%S")

# Define local CSV file path and target volume path
local_file_path = "extract/upload/raw_transactions_UPLOAD.csv"  # Replace with your local CSV file path
catalog_name = "pyspark_tut"  # Replace with your Unity Catalog name
schema_name = "loyalty_program"  # Replace with your schema name
volume_name = "loyalty_pipeline"  # Replace with your volume name
target_file_name = (
    f"uploaded_file_{_time_ref}.csv"  # Desired name for the uploaded file
)

volume_file_path = (
    f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{target_file_name}"
)


def file_checker() -> bool:
    """Check if the file for upload exists.

    Returns:
        bool: True if the file exists
    """
    file_path = Path(local_file_path)

    try:
        if file_path.exists() and file_path.is_file():
            logger.info(f"File was found: {file_path}")
        else:
            logger.error(f"File not found: {file_path}")
            raise
    except Exception as e:
        logger.error(f"Error checking file: {e}")
        raise

    return True


def file_uploader() -> bool:
    """Upload file to Databrick Unity Catalog Volume.

    Returns:
        bool: True if no error encountered.
    """
    # Check file
    file_checker()

    try:
        logger.info("Reading file.")

        # Read the local CSV file into bytes
        with open(local_file_path, "rb") as f:
            file_bytes = f.read()
        binary_data = io.BytesIO(file_bytes)

        # Upload the file to the Unity Catalog volume
        # Overwrite the file with the same name.
        w.files.upload(volume_file_path, binary_data, overwrite=True)
        logger.info("Uploading file.")

        logger.info(
            f"Successfully uploaded '{local_file_path}' to '{volume_file_path}'"
        )

    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise

    return True
