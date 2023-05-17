import os

# monitoring.py
DIRECTORY_TO_WATCH = "./data"
SCRIPT_TO_EXECUTE = "./scripts/log_processor.py"

# log_processor.py

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

SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
RECEIVER_EMAIL = os.getenv('RECEIVER_EMAIL')