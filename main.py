from app.consumer.task_manager import TaskManager


if __name__ == "__main__":
    task_manager: TaskManager = TaskManager()
    try:
        task_manager.consume()
    except KeyboardInterrupt:
        print("Diconnecting and stopping all consumers...")
        task_manager.stop_consuming()
        print("Consumers stopped successfully.")