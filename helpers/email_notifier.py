import smtplib
import ssl
import os
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email_html_template import html_template
import logging
from logger_config import setup_logging
import re
from email_validator import validate_email, EmailNotValidError

# Load environment variables
load_dotenv()


# Logging
setup_logging()
logger = logging.getLogger(__name__)


def email_check(email_address) -> str:
    """This will check if the email address is valid.

    Args:
        email_address (str): Customer email address to be checked

    Returns:
        str: Customer's email address
    """
    try:
        email_info = validate_email(
            email_address, check_deliverability=False
        )  # This will make a DNS query to check the domain
        logger.info("Validating Email address.")

        email = email_info.normalized
    except EmailNotValidError as e:
        logger.error(f"Email address is not valid: {e}")
        raise

    return email
