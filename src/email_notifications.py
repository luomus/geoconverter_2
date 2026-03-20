import logging
import smtplib
import traceback
from email.mime.text import MIMEText
from datetime import datetime
import settings

# Setup logging
app_settings = settings.Settings()
log_level = getattr(logging, app_settings.LOGGING.upper(), logging.DEBUG)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

def send_simple_email(subject: str, message: str) -> bool:
    """Send a simple email notification."""

    logging.debug(f"Preparing to send email with subject: {subject}")

    if not app_settings.EMAIL_ENABLED:
        logging.debug("Email notifications are disabled")
        return False

    try:
        # Get admin emails
        recipients = [email.strip() for email in app_settings.ADMIN_EMAILS.split(',') if email.strip()]
        if not recipients:
            logging.warning("No admin emails configured")
            return False

        # Create simple email
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = app_settings.FROM_EMAIL
        msg['To'] = ', '.join(recipients)

        # Send email
        with smtplib.SMTP(app_settings.SMTP_HOST, app_settings.SMTP_PORT) as server:
            if app_settings.SMTP_TLS:
                server.starttls()
                logging.debug("Started TLS for SMTP connection")

            if app_settings.SMTP_USER and app_settings.SMTP_PASSWORD:
                server.login(app_settings.SMTP_USER, app_settings.SMTP_PASSWORD)
                logging.debug("Logged in to SMTP server")

            server.send_message(msg)

        return True

    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return False

def notify_failure(error_message: str, conversion_id: str = "", details: dict = None) -> bool:
    """Send notification when something fails."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = "Geoconverter Error"

    if conversion_id:
        subject += f" - ID: {conversion_id}"

    details_section = ""
    if details:
        details_section = "\n" + "\n".join(f"        {k}: {v}" for k, v in details.items()) + "\n"

    tb = traceback.format_exc()
    traceback_section = f"\n        Traceback:\n{tb}" if tb and tb.strip() != "NoneType: None" else ""

    message = f"""
        Geoconverter Error Report

        Time: {timestamp}
        Environment: {app_settings.CONVERTER_ENV}
        Conversion ID: {conversion_id or 'N/A'}{details_section}
        Error: {error_message}{traceback_section}

        Please check the application logs for more details.
    """

    return send_simple_email(subject, message)
