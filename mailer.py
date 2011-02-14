import sys
import datetime
import smtplib
from email.mime.text import MIMEText
import config.email

class ErrorMailer:
    def __init__(self,message):
        self.msg = MIMEText(message)
        self.msg['Subject'] = 'Pipeline notification at: '+ datetime.datetime.now().strftime("%a %d %b, %I:%M:%S%P")
        
        if not config.email.sender:
            self.msg['From'] = 'noreply@PRESTO-PIPELINE.app'
        else:
            self.msg['From'] = config.email.sender
        self.msg['To'] = config.email.recipient
        self.client = smtplib.SMTP(config.email.smtp_host,587)
        
    def send(self):
            self.client.ehlo()
            self.client.starttls()
            self.client.login(config.email.smtp_username,config.email.smtp_password)
            self.client.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())
            self.client.quit()
    
    def __str__(self):
        return self.msg.as_string()
