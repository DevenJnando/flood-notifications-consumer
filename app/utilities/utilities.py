def set_subject_and_colour(severity_level: int) -> tuple[str, str]:
    subject = "No subject"
    colour: str = "#ffffff"
    match severity_level:
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
    return subject, colour