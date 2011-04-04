################################################################
# Email Notification Configuration
################################################################
enabled = True   # whether error email is sent or not
smtp_host = 'smtp.gmail.com' # None - For use of the local smtp server
smtp_username = 'username'
smtp_password = 'password'
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
