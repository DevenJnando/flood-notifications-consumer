import json
import logging

import pika
from pika.exceptions import AMQPConnectionError

from app.utilities.utilities import set_subject_and_colour
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import BasicProperties

from app.notifications.email_notification_service import send_notification_email


class Consumer:


    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dead_emails = logging.getLogger(__name__)
        self.flood_key: str = "flood"
        self.subscriber_email_key: str = "subscriber_email"
        self.flood_area_id_key: str = "floodAreaID"
        self.flood_description_key: str = "description"
        self.flood_severity_key: str = "severity"
        self.flood_severity_level_key: str = "severityLevel"
        self.flood_message_key: str = "message"
        try:
            self.connection: BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel: BlockingChannel = self.connection.channel()
            self.channel.queue_declare(queue='email', durable=True, arguments={"x-queue-type": "quorum"})
        except AMQPConnectionError as e:
            self.logger.error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e


    def consume(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='email', auto_ack=False, on_message_callback=self.callback)
        self.channel.start_consuming()


    def stop_consuming(self):
        self.channel.stop_consuming()


    def callback(self, channel: BlockingChannel, method, properties: BasicProperties, body: bytes):
        deserialized_body: dict = json.loads(body.decode('utf-8'))
        deserialized_flood: dict = deserialized_body.get(self.flood_key)
        deserialized_subscriber_email: str = deserialized_body.get(self.subscriber_email_key)
        flood_area_id: str = deserialized_flood.get(self.flood_area_id_key)
        flood_description: str = deserialized_flood.get(self.flood_description_key)
        severity: str = deserialized_flood.get(self.flood_severity_key)
        severity_level: int = int(deserialized_flood.get(self.flood_severity_level_key))
        message: str = deserialized_flood.get(self.flood_message_key)
        subject_colour_tuple: tuple[str, str] = set_subject_and_colour(severity_level)
        subject: str = subject_colour_tuple[0]
        colour: str = subject_colour_tuple[1]
        try:
            send_notification_email(deserialized_subscriber_email, subject, flood_area_id, flood_description,
                                    severity, message, colour)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.logger.error(f"Email notification service has failed for subscriber "
                              f"with the following email address: {deserialized_subscriber_email} \n")
            if properties.headers is None:
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            elif properties.headers.get("x-delivery-count") < 20:
                print(properties.headers.get("x-delivery-count"))
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            else:
                self.dead_emails.error(f"Message retry limit reached. Subscriber with email address "
                                  f"{deserialized_subscriber_email} could not be sent.")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
