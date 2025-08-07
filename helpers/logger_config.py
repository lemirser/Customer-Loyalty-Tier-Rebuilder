import os
import logging
from dotenv import load_dotenv


def setup_logging():
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = os.getenv("LOG_FORMAT", "%(levelname)s - %(message)s")
    log_file = os.getenv("LOG_FILE", "logs.log")

    if log_file:
        logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    else:
        logging.basicConfig(level=log_level, format=log_format)
