from mailer import ErrorMailer

m = ErrorMailer("It would seem the mailer is working...", enabled=True)
m.send()
