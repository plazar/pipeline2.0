import jobtracker


def main():
    ready_for_upload = jobtracker.query("SELECT * FROM job_uploads WHERE status='checked'")
    waiting_check = jobtracker.query("SELECT * FROM job_uploads WHERE status='new'")
    for ru in ready_for_upload:
        print "%s\t%s" % (ru['job_id'],"Checked and ready for upload.")
    for ru in ready_for_upload:
        print "%s\t%s" % (ru['job_id'],"Processed and rady to be checked.")
        
    print "\nNum. jobs ready for    upload: %u" % len(ready_for_upload)
    print "Num. jobs waiting for dry-run: %u" % len(waiting_check)


if __name__ == "__main__":
    main()
