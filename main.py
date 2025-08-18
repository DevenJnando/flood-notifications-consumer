from notifications.email_notification_service import send_notification_email


if __name__ == "__main__":
    send_notification_email("crenando_178ma@mailsac.com", "Flood Notification", "TEST-AREA", "Flood Warning", "Test message", "#ff0000")