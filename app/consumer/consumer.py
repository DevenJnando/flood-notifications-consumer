import json

import pika
from app.notifications.email_notification_service import send_notification_email


class Consumer:


    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='email', durable=True)
        self.flood_key = "flood"
        self.subscriber_emails_key = "subscriber_emails"
        self.flood_area_id_key = "floodAreaID"
        self.flood_severity_key = "severity"
        self.flood_severity_level_key = "severityLevel"
        self.flood_message_key = "message"


    def consume(self):
        self.channel.basic_consume(queue='email', auto_ack=True, on_message_callback=self.callback)
        self.channel.start_consuming()


    def stop_consuming(self):
        self.channel.stop_consuming()


    def callback(self, channel, method, properties, body):
        deserialized_body: dict = json.loads(body.decode('utf-8'))
        deserialized_flood: dict = deserialized_body.get(self.flood_key)
        deserialized_subscriber_emails: list[str] = deserialized_body.get(self.subscriber_emails_key)
        flood_area_id: str = deserialized_flood.get(self.flood_area_id_key)
        severity: str = deserialized_flood.get(self.flood_severity_key)
        severityLevel: str = deserialized_flood.get(self.flood_severity_level_key)
        message: str = deserialized_flood.get(self.flood_message_key)
        subject: str = "No Subject"
        colour: str = "#ffffff"
        match severityLevel:
            case 1:
                subject = "Automated Flood Notification - Severe"
                colour = "#ff0000"
            case 2:
                subject = "Automated Flood Notification - Warning"
                colour = "#ff751a"
            case 3:
                subject = "Automated Flood Notification - Alert"
                colour = "#ffcc00"
            case 4:
                subject = "Automated Flood Notification - No longer in force"
                colour = "#0099cc"
        for subscriber_email in deserialized_subscriber_emails:
            send_notification_email(subscriber_email, subject, flood_area_id, severity, message, colour)