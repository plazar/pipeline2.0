#!/usr/bin/env python

import time
import sys
import traceback

import mailer
import Downloader
import config.background
import config.email

def main():
    while True:
        try:
            Downloader.status()
            Downloader.run()
        except Exception, e:
            if config.email.send_on_crash:
                msg  = '*** Downloader has crashed! ***\n\n'
                msg += 'Fatal error occured while running downloader: %s\n\n' % str(e)
                msg += ''.join(traceback.format_exception(*sys.exc_info()))
                notification = mailer.ErrorMailer(msg, subject="Downloader crash!")
                notification.send()
            sys.stderr.write("Fatal error occurred!\n")
            raise
        time.sleep(config.background.sleep)


if __name__=='__main__':
    try:
        main()
    except KeyboardInterrupt:
        print "Exiting..."
