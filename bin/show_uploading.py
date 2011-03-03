import jobtracker
"""
This script displays jobs which results are ready to be uplaoded,
waiting to be checked and uploaded by JobUploader module, with
summary of number of results waiting to be checked, ready for
upload and uploaded.
"""

def main():
    ready_for_upload = jobtracker.query("SELECT * FROM job_uploads WHERE status='checked'")
    waiting_check = jobtracker.query("SELECT * FROM job_uploads WHERE status='new'")
    uploaded = jobtracker.query("SELECT * FROM job_uploads WHERE status='uploaded'")
    for ru in ready_for_upload:
        print "%s\t%s" % (ru['job_id'],"Checked and ready for upload.")
    for ru in waiting_check:
        print "%s\t%s" % (ru['job_id'],"Processed and ready to be checked.")
    for ru in uploaded:
        print "%s\t%s" % (ru['job_id'],"Processed and Uploaded.")

    print "\nNum. of jobs ready for    upload: %u" % len(ready_for_upload)
    print "Num. of jobs waiting for dry-run: %u" % len(waiting_check)
    print "Num. of uploaded jobs : %u" % len(uploaded)


if __name__ == "__main__":
    main()