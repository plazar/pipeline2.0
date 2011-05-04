#!/usr/bin/env python
import datetime
from mailer import ErrorMailer

m = ErrorMailer("It would seem the mailer is working...", \
                    subject='Pipeline test email', enabled=True)
m.send()
