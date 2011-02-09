import sqlite3
import time
import config

def query(query_string):
        not_connected = True
        while not_connected:
            try:
                db_conn = sqlite3.connect(config.bgs_db_file_path,timeout=40.0);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(query_string)
                if db_cur.lastrowid:
                    results = db_cur.lastrowid
                else:
                    results = db_cur.fetchall()
                db_conn.commit()
                db_conn.close()
                not_connected = False
            except Exception, e:
                try:
                    db_conn.close()
                except Exception, e:
                    pass
                print "Couldn't connect to DB retrying in 1 sec.: %s" % str(e) 
                time.sleep(1)
        return results
    

def main():
    ready_for_upload = query("SELECT * FROM job_uploads WHERE status='checked'")
    waiting_check = query("SELECT * FROM job_uploads WHERE status='new'")
    for ru in ready_for_upload:
        print "%s\t%s" % (ru['job_id'],"Checked and ready for upload.")
    for ru in ready_for_upload:
        print "%s\t%s" % (ru['job_id'],"Processed and rady to be checked.")
        
    print "\nNum. jobs ready for    upload: %u" % len(ready_for_upload)
    print "Num. jobs waiting for dry-run: %u" % len(waiting_check)

if __name__ == "__main__":
    main()