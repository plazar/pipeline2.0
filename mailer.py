import sys
import datetime
import smtplib
from email.mime.text import MIMEText

try:
    import mail_cfg
except ImportError:
    sys.stderr.write("\nPipeline mailer requires mail_cfg.py to exist and be on PYTHONPATH.\n")
    sys.stderr.write("Please consult the mail_cfg_example.py for Mailer configuration.\n")
    sys.stderr.write("\nExiting...\n\n")
    sys.exit(1)

try:
    mail_cfg.mailer_enabled
    mail_cfg.mailer_smtp_host
    mail_cfg.mailer_smtp_username
    mail_cfg.mailer_smtp_password
    mail_cfg.mailer_to
    mail_cfg.mailer_from
except AttributeError:
    sys.stderr.write("\nPipeline mailer configuration is missing.\n")
    sys.stderr.write("The following variables must be defined:\n")
    sys.stderr.write("\tmailer_enabled - type: Boolean\n")
    sys.stderr.write("\tmailer_smtp_host - type: String\n")
    sys.stderr.write("\tmailer_smtp_username - type: String\n")
    sys.stderr.write("\tmailer_smtp_password - type: String\n")
    sys.stderr.write("\tmailer_to - type: String\n")
    sys.stderr.write("\tmailer_from - type:String\n")
    sys.stderr.write("\nExiting...\n\n")
    sys.exit(1)

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
