import JobUploader
from time import sleep

upl = JobUploader.JobUploader()

while True:
    upl.run()
    sleep(600)