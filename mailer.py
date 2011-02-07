import os
import datetime
import smtplib
import mail_cfg
from email.mime.text import MIMEText

class ErrorMailer:
    
    def __init__(self,message):
        self.msg = MIMEText(message)
        self.msg['Subject'] = 'Pipeline notification at: '+ datetime.datetime.now().strftime("%a %d %b, %I:%M%P")
        
        if not mail_cfg.mailer_from:
            self.msg['From'] = 'noreply@PRESTO-PIPELINE.app'
        else:
            self.msg['From'] = mail_cfg.mailer_from
        self.msg['To'] = mail_cfg.mailer_to
        self.client = smtplib.SMTP(mail_cfg.mailer_smtp_host,587)
        
    def send(self):
        if mail_cfg.mailer_enabled:
            self.client.ehlo()
            self.client.starttls()
            self.client.login(mail_cfg.mailer_smtp_username,mail_cfg.mailer_smtp_password)
            self.client.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())
            self.client.quit()
    
    def __str__(self):
        return self.msg.as_string()
