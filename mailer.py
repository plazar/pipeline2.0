import os
if os.path.exists('mail_cfg.py'):
    import mail_cfg
    try:
        mail_cfg.mailer_enabled
    except AttributeError:
        exit("mail_cfg.py must contain a setting for mailer_enabled")
        
    try:
        mail_cfg.mailer_smtp_host
    except AttributeError:
        exit( "mail_cfg.py must contain a setting for mailer_smtp_host")
        
    try:
        mail_cfg.mailer_smtp_username
    except AttributeError:
        exit( "mail_cfg.py must contain a setting for mailer_smtp_username")
        
    try:
        mail_cfg.mailer_smtp_password
    except AttributeError:
        exit( "mail_cfg.py must contain a setting for mailer_smtp_password")
        
    try:
        mail_cfg.mailer_to
    except AttributeError:
        exit( "mail_cfg.py must contain a setting for mailer_to")
        
    try:
        mail_cfg.mailer_from
    except AttributeError:
        exit( "mail_cfg.py must contain a setting for mailer_from")
    
else:
    print "Please consult the mail_cfg_example.py for Mailer configuration.\n Then rename the file to mail_cfg.py once completed the configuration"

import datetime
import smtplib
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
