import json

import pika
from pika.exceptions import AMQPConnectionError
from python_http_client import BadRequestsError
from multiprocessing import Process

from app.logging.log import get_logger
from app.utilities.utilities import set_subject_and_colour
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import BasicProperties

from app.notifications.email_notification_service import send_notification_email


class Consumer(Process):
    """
    RabbitMQ Consumer object. Extends the Process class and runs on a dedicated core.
    """


    def __init__(self, max_messages: int):
        """
        Initialize the Consumer object.

        Keys for the relevant flood, flood severity,
        severity level, flood description and flood message are initialized here.
        Subscriber ID and email keys are also initialized.

        A connection to the RabbitMQ broker can be established using credentials.
        :param max_messages: The maximum number of messages to process before self-termination.
        """
        Process.__init__(self)
        if max_messages <= 0:
            raise ValueError("max_messages must be a positive integer")
        self.max_messages = max_messages
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
            get_logger().error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e


    def run(self):
        """
        Called upon initialization of the Consumer object.

        Processes all messages from the queue until the max message limit is reached.
        Self-terminates upon reaching the max message limit.
        :return:
        """
        current_message_count: int = 0
        for method_frame, properties, body in self.channel.consume(queue='email', inactivity_timeout=5):
            current_message_count += 1
            if current_message_count <= self.max_messages:
                self.callback(method_frame, properties, body)
            else:
                break
        get_logger().info("All messages processed")
        self.channel.cancel()
        self.stop_consuming()
        return 0


    def stop_consuming(self):
        """
        Stop consuming from rabbitmq, close the connection and terminate process.
        :return:
        """
        self.channel.close()
        self.connection.close()


    def callback(self, method, properties: BasicProperties, body: bytes):
        """
        Callback which is called when a message is received.
        Decodes and passes the message from the queue to the notify function.

        :param method: Delivery and general message/queue information
        :param properties: Optional properties from message
        :param body: Contents of the message
        :return:
        """
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
            get_logger().error(f"One or more attempts to deserialize message failed. "
                                   f"Rejecting message as subsequent attempts will also fail."
                                   f"{e}")
            self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


    def notify(self, method, properties: BasicProperties, subscriber_id:str, email: str, subject: str,
               flood_area_id: str, flood_description: str, severity: str, message: str, colour: str):
        """
        Takes decoded message information and sends it to the send_notification_email function.
        Upon receipt, an email will be sent to the appropriate address along with all flood information.

        :param method: Delivery and general message/queue information
        :param properties: Optional properties from message
        :param subscriber_id: Subscriber ID
        :param email: Email address
        :param subject: Email subject
        :param flood_area_id: Flood area ID number
        :param flood_description: Flood description
        :param severity: Flood severity level
        :param message: Flood message
        :param colour: Colour to make the button which points to the flood map
        :return:
        """
        try:
            send_notification_email(subscriber_id, email, subject, flood_area_id, flood_description,
                                    severity, message, colour)
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        except BadRequestsError:
            get_logger().error(f"Email notification service has failed for subscriber "
                              f"with the following email address: {email} \n")
            if properties.headers is None:
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            elif properties.headers.get("x-delivery-count") < 20:
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            else:
                get_logger().error(f"Message retry limit reached. Subscriber with email address "
                                       f"{email} could not be sent.")
                self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
