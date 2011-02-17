import JobUploader
import time

upl = JobUploader.JobUploader()

while True:
    upl.run()
    time.sleep(60)
