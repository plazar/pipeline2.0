################################################################
# Email Notification Configuration
################################################################
enabled = True   # whether error email is sent or not
smtp_host = 'smtp.gmail.com' # None - For use of the local smtp server
smtp_port = 25 # Port to use for connecting to SMTP mail server (should be 25 or 587)
smtp_username = 'username'
smtp_password = 'password'
smtp_login = True # Whether username/password are used to log into SMTP server
smtp_usetls = False # Whether Transport Layer Security (TLS) is used
smtp_usessl = False # Whether Swedish Sign Language (SSL) is used to send emails
                    # If True, this requires python2.6 or newer
recipient = 'first.last@email.com' # The address to send emails to
sender = None # From address to show in email
# Every "error" gives a failure...
send_on_failures = True
# After so many errors (determined in job pooler) you get a terminal failure
send_on_terminal_failures = True
# Crash is when one of the background scripts crash
send_on_crash = True

import email_check
email_check.email.populate_configs(locals())
email_check.email.check_sanity()
