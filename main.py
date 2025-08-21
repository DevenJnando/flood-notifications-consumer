from app.consumer.email_consumer import Consumer
from app.consumer.task_manager import TaskManager

if __name__ == "__main__":
    #consumer: Consumer = Consumer()
    task_manager: TaskManager = TaskManager()
    try:
        task_manager.consume()
        #consumer.consume()
    except KeyboardInterrupt:
        print("Diconnecting and stopping all consumers...")
        task_manager.stop_consuming()
        #consumer.stop_consuming()
        print("Consumers stopped successfully.")