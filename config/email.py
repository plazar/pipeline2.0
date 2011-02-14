################################################################
# Email Notification Configuration
################################################################
enabled = True
smtp_host = 'smtp.gmail.com' # None - For use of the local smtp server
smtp_username = 'mcgill.pipeline@gmail.com'
smtp_password = 'mcg1592l!!'
recipient = 'patricklazarus@gmail.com' # The address to send emails to
sender = None # From address to show in email
send_on_failures = True
send_on_terminal_failures = True

import email_check
email_check.email.populate_configs(locals())
email_check.email.check_sanity()
