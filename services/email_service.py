import smtplib
import os
from services.logger_service import logger
from services.runtime_env_config_service import read_configs

configs = read_configs()

HOST = configs["ALERT"]["host.server"]
PORT = configs["ALERT"]["host.port"]
receivers = configs["ALERT"]["host.receivers"]
# if we want to send email alert to more than 1 person then in configs we will pass ; separated email addresses
receivers = list(map(str.strip,receivers.split(';'))) 

# Fetching sender email and password from environment variable
SENDER = os.environ.get("service_account")
PASSWORD = os.environ.get("service_account_password")


def send_mail(message):
    try:
        logger.info(f"Email send for {message}")
        with smtplib.SMTP(HOST,PORT) as smtp_client:
            smtp_client.starttls()
            smtp_client.login(SENDER,PASSWORD)
            smtp_client.sendmail(SENDER, receivers,message)
            smtp_client.quit()
    except Exception as e:
        logger.error(f"Email Notification Send Failure for message: {message}.\nError: {e}")


