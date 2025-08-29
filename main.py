from app.consumer.task_manager import TaskManager
from app.logging.log import get_logger

if __name__ == "__main__":
    task_manager: TaskManager = TaskManager()
    try:
        get_logger().info("Starting consumer task manager...")
        task_manager.consume()
    except KeyboardInterrupt:
        get_logger().info("Stopping consumer task manager...")
        task_manager.stop_consuming()
        get_logger().info("Consumer task manager stopped.")