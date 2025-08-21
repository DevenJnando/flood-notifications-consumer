import json
import logging
import math
import multiprocessing

import pika
from pika import BlockingConnection, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from python_http_client import BadRequestsError

from app.consumer.email_consumer import Consumer


MAX_TASKS_PER_QUEUE = 100


class TaskManager:


    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dead_messages = logging.getLogger(__name__)
        self.no_of_tasks_key = "no_of_tasks"
        try:
            self.connection: BlockingConnection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel: BlockingChannel = self.connection.channel()
            self.channel.queue_declare(queue='tasks', durable=True,
                                       arguments={"x-queue-type": "quorum"})
        except AMQPConnectionError as e:
            self.logger.error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e


    def consume(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='tasks', auto_ack=False, on_message_callback=self.callback)
        self.channel.start_consuming()


    def stop_consuming(self):
        self.channel.stop_consuming()
        self.channel.close()
        self.connection.close()


    def callback(self, channel: BlockingChannel, method, properties: BasicProperties, body: bytes):
        try:
            deserialized_body: dict = json.loads(body.decode('utf-8'))
            no_of_tasks: int = deserialized_body.get(self.no_of_tasks_key)
            no_of_workers: int = math.ceil(no_of_tasks / MAX_TASKS_PER_QUEUE)
            no_of_workers = no_of_workers if no_of_workers <= multiprocessing.cpu_count() else multiprocessing.cpu_count()
            tasks_per_worker: int = math.ceil(no_of_tasks / no_of_workers)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.manage_workers(no_of_workers, tasks_per_worker)
        except (AttributeError, ValueError) as e:
            self.dead_messages.error(f"Attempts to deserialize message failed. "
                                     f"Rejecting message as subsequent attempts will also fail."
                                     f"{e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


    def manage_workers(self, no_of_workers: int, tasks_per_worker: int):
        try:
            workers: list[Consumer] = []
            for i in range(no_of_workers):
                worker: Consumer = Consumer(tasks_per_worker)
                workers.append(worker)
            for worker in workers:
                try:
                    worker.start()
                    print(worker.pid)
                except Exception as e:
                    self.logger.error(e)
        except AMQPConnectionError as e:
            self.logger.error("Could not connect to rabbitmq. Ensure rabbitmq is running.\n"
                              f"AMQPConnectionError: {e}")
            raise e