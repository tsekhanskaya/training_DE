import smtplib

from email.mime.text import MIMEText
from logger import logger


class AlertSender:
    def __init__(self, sender_email, sender_password, receiver_email):
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.receiver_email = receiver_email

    def send_alert(self, subject, message):
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = self.receiver_email

        try:
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(self.sender_email, self.sender_password)
                smtp.sendmail(self.sender_email, self.receiver_email, msg.as_string())
            logger.info("Alert sent successfully!")
        except Exception as e:
            logger.error(f"Failed to send alert. Error: {e}")
