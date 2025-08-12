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
        )  # This will make a DNS query to check the domain if set to True
        logger.info("Validating Email address.")

        email = email_info.normalized
    except EmailNotValidError as e:
        logger.error(f"Email address [{email_address}] is not valid: {e}")
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

    result = (clean_receiver_emails, clean_subject, email_provider)

    logger.info("Done checking the parameters.")

    return result


def send_email(
    sender_email,
    sender_password,
    receiver_emails,
    html_content,
    subject="Loyalty Program Update",
    email_provider="gmail",
):
    """
    Sends an HTML email to one or multiple recipients using SMTP with SSL.

    Args:
        sender_email (str): Your email address (must match SMTP login).
        sender_password (str): Your email password or app-specific password.
        receiver_emails (list[str]): List of recipient email addresses.
        subject (str): Email subject.
        html_content (str): HTML content of the email body.
    """
    # Sanitize
    clean_receiver_emails, clean_subject, clean_email_provider = sanitize_parameters(
        receiver_emails, subject, email_provider
    )

    # Create email container
    message = MIMEMultipart("alternative")  # allows both plain and HTML versions
    message["From"] = sender_email
    message["To"] = clean_receiver_emails
    message["Subject"] = clean_subject

    # Attach plain text and HTML versions
    message.attach(MIMEText(html_content, "html"))

    # SMTP secure connection
    if clean_email_provider == "gmail":
        smtp_server = os.getenv("GMAIL_SMTP")
        port = os.getenv("GMAIL_PORT")
        context = os.getenv("GMAIL_CONTEXT")
    elif clean_email_provider == "outlook":
        smtp_server = os.getenv("OUTLOOK_SMTP")
        port = os.getenv("OUTLOOK_PORT")
        context = os.getenv("EMAIL_CONTEXT")
    elif clean_email_provider == "yahoo":
        smtp_server = os.getenv("YAHOO_SMTP")
        port = os.getenv("YAHOO_PORT")
        context = os.getenv("EMAIL_CONTEXT")

    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, message.as_string())
        logger.info("Successfully sent the email.")
    except Exception as e:
        logging.error(
            f"There was an issue sending the email {clean_email_provider}: {e}"
        )
        raise
