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


def sanitize_parameters(
    receiver_emails,
    subject="Loyalty Program Update",
    email_provider="gmail",
) -> tuple:
    """Sanitize any invalid characters.

    Args:
        receiver_emails (str): customer email address
        subject (str, optional): Email Subject line. Defaults to "Loyalty Program Update".
        email_provider (str, optional): Email provider for the sender. Defaults to "gmail".

    Returns:
        tuple: Returns the sanitized parameters
    """
    logger.info("Sanitizing parameters.")

    if len(subject) > 30:
        logging.error("Subject line should only contain 30 or less characters.")
        raise

    if email_provider.strip().lower() not in ["gmail", "yahoo", "outlook"]:
        logging.error(f"Invalid email provider: {email_provider}")
        raise

    clean_subject = re.sub(r"[^a-zA-Z0-9]", "", subject)
    clean_receiver_emails = email_check(receiver_emails)

    result = (email_provider, clean_receiver_emails, clean_subject)

    logger.info("Done checking the parameters.")

    return result
