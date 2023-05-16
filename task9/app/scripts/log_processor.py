import pandas as pd
import sys
import os

from alert_sender import AlertSender
from error_analyzer import ErrorAnalyzer
from logger import logger


FILENAME = sys.argv[1]
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
RECEIVER_EMAIL = os.getenv('RECEIVER_EMAIL')


HEADERS = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id', 'sdkv',
           'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language',
           'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']

DTYPES = {
    'error_code': str,
    'error_message': str,
    'severity': str,
    'log_location': str,
    'mode': str,
    'model': str,
    'graphics': str,
    'session_id': str,
    'sdkv': str,
    'test_mode': str,
    'flow_id': str,
    'flow_type': str,
    'sdk_date': str,
    'publisher_id': str,
    'game_id': str,
    'bundle_id': str,
    'appv': str,
    'language': str,
    'os': str,
    'adv_id': str,
    'gdpr': str,
    'ccpa': str,
    'country_code': str,
    'date': str
}


class LogProcessor:
    def __init__(self, filename, sender_email, sender_password, receiver_email):
        self.filename = filename
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.receiver_email = receiver_email
        self.alert_sender = AlertSender(sender_email, sender_password, receiver_email)

    def filter_fatal(self) -> None:
        """
        Since the number of lines can exceed the memory for processing by pandas, a file read divided by
        1,000,000 lines is used. Further processing goes in parts of the dataframe
        :return: None
        """
        with pd.read_csv(f'./data/{self.filename}', chunksize=1000000, names=HEADERS, dtype=DTYPES) as reader:
            count = 1
            for chunk in reader:
                logger.info(f"Processing chunk {count} of {self.filename}")
                chunk.dropna(how='all', inplace=True)
                filtered_df = chunk[chunk['severity'].str.lower().isin(['error'])]
                analyzer = ErrorAnalyzer(filtered_df, self)
                analyzer.format()
                analyzer.one_minute()
                analyzer.one_hour()

                logger.info(f"Processed part {count} of {self.filename} successfully!")
                count += 1
        logger.info(f"Processed of {self.filename} successfully!")

    def send_alert(self, subject, message):
        self.alert_sender.send_alert(subject, message)


logger.info(f"{FILENAME} file processing started")

log_processor = LogProcessor(FILENAME, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL)
log_processor.filter_fatal()
