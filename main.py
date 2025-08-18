from app.consumer.consumer import Consumer

if __name__ == "__main__":
    consumer: Consumer = Consumer()
    try:
        consumer.consume()
    except KeyboardInterrupt:
        print("Diconnecting and stopping all consumers...")
        consumer.stop_consuming()
        print("Consumers stopped successfully.")