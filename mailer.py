import os
import datetime
import smtplib
import config
from email.mime.text import MIMEText

class ErrorMailer:
    
    def __init__(self,message):
        self.msg = MIMEText(message)
        self.msg['Subject'] = 'Pipeline notification at: '+ datetime.datetime.now().strftime("%a %d %b, %I:%M%P")
        
        if not config.mailer_from:
            self.msg['From'] = 'noreply@PRESTO-PIPELINE.app'
        else:
            self.msg['From'] = config.mailer_from
        self.msg['To'] = config.mailer_to
        self.client = smtplib.SMTP(config.mailer_smtp_host,587)
        
    def send(self):
        self.client.starttls()
        self.client.login(config.mailer_smtp_username,config.mailer_smtp_password)
        self.client.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())
        self.client.quit()
    
    def __str__(self):
        return self.msg.as_string()