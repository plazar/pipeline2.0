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
    processing = query("SELECT *,job_submits.created_at as job_submit_created_at FROM job_submits,jobs,job_files,downloads WHERE job_submits.status='running' AND jobs.id=job_submits.job_id AND job_files.job_id=jobs.id AND downloads.id=job_files.file_id")
    for job in processing:
        print "%s\t%s\t%s" % (job['filename'],job['queue_id'],job['job_submit_created_at'])

if __name__ == "__main__":
    main()