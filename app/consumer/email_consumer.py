import json
import logging

import pika
from pika.exceptions import AMQPConnectionError
from python_http_client import BadRequestsError
from multiprocessing import Process

from app.utilities.utilities import set_subject_and_colour
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import BasicProperties

from app.notifications.email_notification_service import send_notification_email


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class Consumer(Process):


    def __init__(self, max_messages: int):
        Process.__init__(self)
        if max_messages <= 0:
            raise ValueError("max_messages must be a positive integer")
        self.max_messages = max_messages
        self.logger = logging.getLogger(__name__)
        self.dead_emails = logging.getLogger(__name__)
        self.max_messages = max_messages
        self.flood_key: str = "flood"
        self.subscriber_id_key: str = "subscriber_id"
        self.subscriber_email_key: str = "subscriber_email"
        self.flood_area_id_key: str = "floodAreaID"
        self.flood_description_key: str = "description"
        self.flood_severity_key: str = "severity"
        self.flood_severity_level_key: str = "severityLevel"
        self.flood_message_key: str = "message"
        try:
            self.connection: BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel: BlockingChannel = self.connection.channel()
            self.channel.queue_declare(queue='email', durable=True,
                                       arguments={"x-queue-type": "quorum"})
            self.email_queue = self.channel.queue_declare(queue='email', durable=True,
                                       arguments={"x-queue-type": "quorum"}, passive=True)
        except AMQPConnectionError as e:
            self.logger.error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e


    def run(self):
        current_message_count: int = 0
        for method_frame, properties, body in self.channel.consume(queue='email', inactivity_timeout=5):
            current_message_count += 1
            if current_message_count <= self.max_messages:
                self.callback(method_frame, properties, body)
            else:
                break
        self.logger.info("All messages processed")
        self.channel.cancel()
        self.stop_consuming()
        return 0


    def stop_consuming(self):
        self.channel.close()
        self.connection.close()


    def callback(self, method, properties: BasicProperties, body: bytes):
        try:
            deserialized_body: dict = json.loads(body.decode('utf-8'))
            deserialized_flood: dict = deserialized_body.get(self.flood_key)
            deserialized_subscriber_id: str = deserialized_body.get(self.subscriber_id_key)
            deserialized_subscriber_email: str = deserialized_body.get(self.subscriber_email_key)
            flood_area_id: str = deserialized_flood.get(self.flood_area_id_key)
            flood_description: str = deserialized_flood.get(self.flood_description_key)
            severity: str = deserialized_flood.get(self.flood_severity_key)
            severity_level: int = int(deserialized_flood.get(self.flood_severity_level_key))
            message: str = deserialized_flood.get(self.flood_message_key)
            subject_colour_tuple: tuple[str, str] = set_subject_and_colour(severity_level)
            subject: str = subject_colour_tuple[0]
            colour: str = subject_colour_tuple[1]
            self.notify(method, properties, deserialized_subscriber_id, deserialized_subscriber_email,
                        subject, flood_area_id, flood_description, severity, message, colour)
        except (AttributeError, ValueError) as e:
            self.dead_emails.error(f"One or more attempts to deserialize message failed. "
                                   f"Rejecting message as subsequent attempts will also fail."
                                   f"{e}")
            self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


    def notify(self, method, properties: BasicProperties, subscriber_id:str, email: str, subject: str,
               flood_area_id: str, flood_description: str, severity: str, message: str, colour: str):
        try:
            send_notification_email(subscriber_id, email, subject, flood_area_id, flood_description,
                                    severity, message, colour)
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        except BadRequestsError:
            self.logger.error(f"Email notification service has failed for subscriber "
                              f"with the following email address: {email} \n")
            if properties.headers is None:
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            elif properties.headers.get("x-delivery-count") < 20:
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            else:
                self.dead_emails.error(f"Message retry limit reached. Subscriber with email address "
                                       f"{email} could not be sent.")
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
