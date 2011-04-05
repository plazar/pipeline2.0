import sys
import os
import datetime
import smtplib
from email.mime.text import MIMEText
import config.email

class ErrorMailer:
    def __init__(self,message):
        self.msg = MIMEText(message.strip())
        self.msg['Subject'] = 'Pipeline notification at: '+ datetime.datetime.now().strftime("%a %d %b, %I:%M:%S%P")
        self.msg['To'] = config.email.recipient
        
        if config.email.smtp_host is None:
            self.msg['From'] = '%s@localhost' % os.getlogin()
            self.client = smtplib.SMTP('localhost', 587)
        else:
            self.msg['From'] = None
            self.client = smtplib.SMTP(config.email.smtp_host, 587)

    def send(self):
        if config.email.enabled:
            self.client.ehlo()
            self.client.starttls()
            if config.email.smtp_host is not None:
                self.client.login(config.email.smtp_username,config.email.smtp_password)
            self.client.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())
            self.client.quit()

    def __str__(self):
        return self.msg.as_string()
