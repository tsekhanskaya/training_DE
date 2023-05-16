import pandas as pd

from logger import logger


class ErrorAnalyzer:
    def __init__(self, df: pd.DataFrame, log_processor):
        self.df = df.copy()
        self.log_processor = log_processor

    def format(self) -> None:
        """
        Convert date to unix system format with seconds.
        :return: None
        """
        self.df['date'] = self.df['date'].replace(',', '.', regex=True).astype(float)
        self.df['date'] = pd.to_datetime(self.df['date'], unit='s')
        self.df.sort_values(by='date', inplace=True)

    def one_minute(self) -> None:
        """
        checking for more than 10 fatal errors in less than one minute.
        :return: None
        """
        self.df.loc[:, 'minute'] = self.df['date'].apply(lambda unix_time: unix_time.minute)

        if (self.df.groupby('minute').count()['error_code'] > 10).any():
            message = "More than 10 fatal errors per minute in the log file!"
            self.log_processor.send_alert("Alert: Fatal Errors per Minute", message)
        logger.info("Checked for one minute")

    def one_hour(self):
        """
        Check for more than 10 fatal errors in less than one hour for a specific bundle_id.
        :return: None
        """
        self.df.loc[:, 'hour'] = self.df['date'].apply(lambda unix_time: unix_time.hour)

        group_data = (self.df.groupby(['bundle_id', 'hour']).count()['error_code'] > 10).groupby('bundle_id').any()
        if group_data.any():
            message = "More than 10 fatal errors per hour for a specific bundle_id in the log file!"
            self.log_processor.send_alert("Alert: Fatal Errors per Hour", message)
        logger.info("Checked for one hour")
