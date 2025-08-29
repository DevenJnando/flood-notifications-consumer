import json
import math
import multiprocessing

import pika
from pika import BlockingConnection, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.credentials import PlainCredentials
from pika.exceptions import AMQPConnectionError

from app.consumer.email_consumer import Consumer
from app.env_vars import rabbitmq_user, rabbitmq_password, rabbitmq_host, rabbitmq_port
from app.logging.log import get_logger


MAX_TASKS_PER_QUEUE = 100


def manage_workers(no_of_workers: int, tasks_per_worker: int):
    """
    Function which spins up the specified number of workers and gives each worker
    the specified number of tasks.

    :param no_of_workers: Number of workers to spawn
    :param tasks_per_worker: Tasks per worker
    :return:
    """
    try:
        workers: list[Consumer] = []
        for i in range(no_of_workers):
            worker: Consumer = Consumer(tasks_per_worker)
            workers.append(worker)
        for worker in workers:
            try:
                worker.start()
            except Exception as e:
                get_logger().error(e)
    except AMQPConnectionError as e:
        get_logger().error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                          f"AMQPConnectionError: {e}")
        raise e


class TaskManager:
    """
    RabbitMQ Consumer Task Manager
    """


    def __init__(self):
        """
        Initializes the Task Manager.
        Establishes a connection to RabbitMQ using credentials.
        """
        self.no_of_tasks_key = "no_of_tasks"
        try:
            credentials: PlainCredentials = pika.PlainCredentials(username=rabbitmq_user, password=rabbitmq_password)
            self.connection: BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbitmq_host,
                port=rabbitmq_port,
                credentials=credentials))
            self.channel: BlockingChannel = self.connection.channel()
            self.channel.queue_declare(queue='tasks', durable=True,
                                       arguments={"x-queue-type": "quorum"})
        except AMQPConnectionError as e:
            get_logger().error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e


    def consume(self):
        """
        Begins consuming messages from the queue
        :return:
        """
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='tasks', auto_ack=False, on_message_callback=self.callback)
        self.channel.start_consuming()


    def stop_consuming(self):
        """
        Stops consuming from the queue, closes the connection and terminates.
        :return:
        """
        self.channel.stop_consuming()
        self.channel.close()
        self.connection.close()


    def callback(self, channel: BlockingChannel, method, properties: BasicProperties, body: bytes):
        """
        Callback function when a message is received from the queue.
        Determines how many workers to spin up based on the total number of tasks (emails)
        which need to be processed.
        Then spins up the workers and divides the work amongst them.

        :param channel: Channel where the message was received
        :param method: Delivery and general message/queue information
        :param properties: Optional properties from message
        :param body: Contents of the message
        :return:
        """
        try:
            deserialized_body: dict = json.loads(body.decode('utf-8'))
            no_of_tasks: int = deserialized_body.get(self.no_of_tasks_key)
            no_of_workers: int = math.ceil(no_of_tasks / MAX_TASKS_PER_QUEUE)
            no_of_workers = no_of_workers if no_of_workers <= multiprocessing.cpu_count() else multiprocessing.cpu_count()
            tasks_per_worker: int = math.ceil(no_of_tasks / no_of_workers)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            manage_workers(no_of_workers, tasks_per_worker)
        except (AttributeError, ValueError) as e:
            get_logger().fatal(f"Attempts to deserialize message failed. "
                               f"Rejecting message as subsequent attempts will also fail."
                               f"{e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

