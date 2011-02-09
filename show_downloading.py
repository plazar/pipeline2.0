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
    downloading = query("SELECT * FROM downloads, download_attempts WHERE download_attempts.status='downloading' AND downloads.id=download_attempts.download_id")
    for download in downloading:
        print "%s\t\t%s" % (download['remote_filename'],download['details'])
    
    print "\nTotal: %u" % len(downloading)
    
if __name__ == "__main__":
    main()