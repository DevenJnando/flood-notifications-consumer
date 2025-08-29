import logging
import os.path
from logging.handlers import RotatingFileHandler

from env_vars import *

log_name = 'flood-consumer-' + BUILD + ".log"
log_path = os.path.expanduser('~') + "/" + LOG_FILE_LOCATION

if not os.path.exists(log_path):
    try:
        os.makedirs(log_path)
    except FileExistsError:
        pass

logger = logging.getLogger(log_name)
file_handler = RotatingFileHandler(log_path+ "/" + log_name,
                                   maxBytes=716800)

if BUILD == 'dev':
    logger.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.DEBUG)
if BUILD == 'prod':
    logger.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def get_logger():
    return logger
