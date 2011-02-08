import os.path
import config
import sqlite3
import sys
import os
from job import PulsarSearchJob
import datetime
import time

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

def get_files(dir_in):
    files = list()
    for entry in os.listdir(dir_in):
        if os.path.isfile(os.path.join(dir_in,entry)):
            if entry[len(entry)-5:] == '.fits':
                files.append(os.path.join(dir_in,entry))
    return files

def get_downloads():
    files = list()
    downloads = query("SELECT * FROM downloads")
    for download in downloads:
        files.append(download['filename'])
    return files

def create_download(file_path):
    filename = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)
    in_query = "INSERT INTO downloads (remote_filename,filename,status,created_at,updated_at,size,details) VALUES ('%s','%s','%s','%s','%s',%u,'%s')"\
                        % (filename,file_path,'downloaded',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(filesize),"Manually added via add_files.py")
    return query(in_query)
        
def usage():
    exit( "\nUsage: python add_files.py [directory to pick up file from]\n")

def main(argv):
    if argv == list():
         usage()
    if os.path.exists(argv[0]):
        adding_dir = argv[0]
        if os.access(adding_dir,os.R_OK):
            files_to_xcheck = get_files(adding_dir)
        else:
            exit("You don't have permission to read %s ." % adding_dir)
    else:
        exit("%s  does not exist." % argv[0])
    
    db_files = get_downloads()
    
    for file_path in files_to_xcheck:
        if file_path not in db_files:
            
            try:
                PulsarSearchJob([file_path]).get_output_dir()
                print "Adding: %s " % file_path
                try:
                    result = create_download(file_path)
                    if isinstance(result,int):
                        print "Download Entry created for: %s with iD: %u" % (file_path,result)
                except Exception,e:
                    print "Couldn't create a download entry for: %s \nReason: %s" % (file_path,str(e))
            except Exception,e:
                print "Couldn't read: %s " % file_path
                pass
    

if __name__ == "__main__":
    main(sys.argv[1:])