################################################################
# Email Notification Configuration
################################################################
enabled = True
smtp_host = 'smtp.gmail.com' # None - For use of the local smtp server
smtp_username = 'username'
smtp_password = 'password'
recipient = 'first.last@email.com' # The address to send emails to
sender = None # From address to show in email
send_on_failures = True
send_on_terminal_failures = True
send_on_crash = True

import email_check
email_check.email.populate_configs(locals())
email_check.email.check_sanity()
