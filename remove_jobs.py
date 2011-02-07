from job import *
import sys
import config
import getopt

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

def usage():
    print "python %s [<options>] [Queued Id [<Queue Id>...]]\n \t -f | --force-fail \t\t Terminates the job with a failed attempt for processing." % __file__
    
def main(argv):
    print "\n\n"
    force_fail = False
    force_fail_str = "Disabled"
    try:                                
        opts, args = getopt.getopt(argv, "f", ["force-fail"]) 
    except getopt.GetoptError:           
        usage()                          
        sys.exit(2)
    for opt, arg in opts:
        if opt in ['--force-fail','-f']:
            force_fail = True
            force_fail_str = "Enabled"
        
    print "Force Fail: %s" % force_fail_str
    
    for job_id in args:
        if QueueManagerClass.is_running(job_id):
            
            job_submit = query("SELECT * FROM job_submits WHERE queue_id='%s' ORDER BY ID DESC LIMIT 1" % job_id)
            if len(job_submit) == 0:
                print("Job submit with id %s not found." % job_id)
                continue
            print "Stopping job: %s" % job_id
            #QueueManagerClass.delete(job_id)
            job = query("SELECT * FROM jobs WHERE id=%u" % job_submit[0]['job_id'])
            if len(job) == 0:
                print("No Job with job submit %s was found in database table 'jobs'." % job_id)
                continue
            
            #if force_fail:
                #query("UPDATE job_submits SET status='failed' WHERE id=%u" % job_submit['id'])
                #query("UPDATE jobs SET status='failed' WHERE id=%u" % job_submit['job_id'])
            #else:
                #query("DELETE job_submits WHERE id=%u" % job_submit['id'])
                #query("DELETE jobs WHERE id=%u" % job_submit['job_id'])
         
        else:
            print("No Job with queue_id %s was found in Queue Manager." % job_id)




if __name__ == "__main__":
    main(sys.argv[1:])