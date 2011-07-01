import sys
import os
import os.path
import datetime
import socket
import smtplib
from email.mime.text import MIMEText
import config.email

class ErrorMailer:
    def __init__(self, message, \
                    subject='Pipeline notification', \
                    enabled=config.email.enabled):

        if not subject.lower().startswith('pipeline notification'):
            subject = 'Pipeline notification: ' + subject

        nowstr = datetime.datetime.now().strftime("%a %d %b, %I:%M:%S%P")
        prog = os.path.split(sys.argv[0])[-1]
        intro = "Pipeline notification from %s on %s at %s\n%s\n" % (prog, \
                    socket.gethostname(), nowstr, '-'*50)
        
        self.enabled = enabled
        self.msg = MIMEText(intro + message.strip())
        self.msg['Subject'] = subject
        self.msg['To'] = config.email.recipient
        if self.enabled:
            if config.email.smtp_usessl:
                # Requires python2.6 or better
                smtp = smtplib.SMTP_SSL
            else:
                smtp = smtplib.SMTP
            if config.email.smtp_host is None:
                self.msg['From'] = '%s@%s' % (os.getenv('USER'), \
                                                socket.gethostname())
                self.client = smtp('localhost', config.email.smtp_port)
            else:
                self.msg['From'] = None
                self.client = smtp(config.email.smtp_host, config.email.smtp_port)

    def send(self):
        if self.enabled:
            self.client.ehlo()
            if config.email.smtp_usetls:
                self.client.starttls()
            self.client.ehlo()
            if config.email.smtp_login:
                self.client.login(config.email.smtp_username,config.email.smtp_password)
            self.client.sendmail(self.msg['From'], self.msg['To'], self.msg.as_string())
            self.client.quit()

    def __str__(self):
        return self.msg.as_string()
