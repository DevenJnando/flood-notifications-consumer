from os import getenv

from dotenv import load_dotenv


load_dotenv()

try:
    API_KEY = getenv('SENDGRID_EMAIL_API_KEY')
    FROM_EMAIL = getenv('SENDGRID_FROM_EMAIL')
    FLOOD_MAP_HOST_NAME = getenv('FLOOD_MAP_HOST_NAME')
    REPLY_EMAIL = getenv('SENDGRID_REPLY_EMAIL')
    rabbitmq_host = getenv("RABBITMQ_HOST")
    rabbitmq_port = getenv("RABBITMQ_PORT")
    rabbitmq_user = getenv("RABBITMQ_USER")
    rabbitmq_password = getenv("RABBITMQ_PASSWORD")
    LOG_FILE_LOCATION = getenv("LOG_FILE_LOCATION")
    BUILD = getenv("BUILD")
except KeyError:
    API_KEY = 'SENDGRID_EMAIL_API_KEY'
    FROM_EMAIL = 'SENDGRID_FROM_EMAIL'
    FLOOD_MAP_HOST_NAME = 'FLOOD_MAP_HOST_NAME'
    REPLY_EMAIL = 'SENDGRID_REPLY_EMAIL'
    rabbitmq_host = "RABBITMQ_HOST"
    rabbitmq_port = "RABBITMQ_PORT"
    rabbitmq_user = "RABBITMQ_USER"
    rabbitmq_password = "RABBITMQ_PASSWORD"
    LOG_FILE_LOCATION = "LOG_FILE_LOCATION"
    BUILD = "BUILD"