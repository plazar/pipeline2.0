import os.path
from ftplib import FTP
from time import sleep
import logging
import sys
import os
from suds.client import Client
from threading import Thread
import shutil
import time
import re
import sqlite3
import datetime
from M2Crypto import ftpslib

from config import downloader_temp, rawdata_directory, downloader_api_password, \
    downloader_api_service_url, downloader_space_to_use, downloader_numofdownloads,\
    downloader_numofrestores,downloader_api_username, downloader_numofretries


class DownloadModule:

    def __init__(self):
        self.LOG_FILENAME = "download_module.log"
        logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=self.LOG_FILENAME,
                    filemode='w')
        self.my_logger = logging.getLogger('MyLogger')
        self.my_logger.info('Initializing.')
       
        self.username = downloader_api_username
        self.password = downloader_api_password
        self.db_name = "sqlite3db"
        self.db_conn = sqlite3.connect("sqlite3db");
        self.db_conn.row_factory = sqlite3.Row
        self.db_cur = self.db_conn.cursor();
        self.restores = []

    def run(self):
        self.recover()
        for res in self.restores:
            print res

        #if can create more restores then request new ones and add them to restores array
        while True:
            while self.can_request_more():
                self.restores.append(restore(db_name=self.db_name,num_beams=1))
            for res in self.restores[:]:
                if not res.run():
                    print "Could not run the restore...deleting"
                    self.restores.remove(res)
                print "Number of restores: "+ str(len(self.restores))
                sleep(2)
            
    def recover(self):
        rec_query = "SELECT * FROM restores WHERE dl_status NOT LIKE 'Finished:%' AND dl_tries < "+ str(downloader_numofretries);
        self.db_cur.execute(rec_query)
        for res_row in self.db_cur:
            self.restores.append(restore(db_name=self.db_name,num_beams=1,guid=res_row['guid']))

        
    def have_space(self):
        return True


    def can_request_more(self):
        if len(self.restores) >= downloader_numofrestores:
            print "Cannot have more than "+ str(downloader_numofrestores) +" at a time."
            return False
        else:
            print "Will request more files"
            return True

    def get_by_restore_guid(self, guid):
        sel_query = "SELECT * FROM restores WHERE guid = '%s' LIMIT 1" % (guid)
        self.db_cur.execute(sel_query)
        return self.db_cur.fetchone()

    def dump_db(self):
        query = "SELECT * FROM restores"
        self.db_cur.execute(query);
        for row in self.db_cur:
            print row
        print ""
        print ""

class restore:

    def __init__(self,db_name, num_beams,guid=False):
        self.values = None
        self.num_beams = num_beams
        self.db_name = db_name
        self.db_conn = sqlite3.connect("sqlite3db");
        self.db_conn.row_factory = sqlite3.Row
        self.db_cur = self.db_conn.cursor();
        self.downloader = False
        self.WebService =  Client(downloader_api_service_url).service
        self.username = downloader_api_username
        self.password = downloader_api_password
        self.remove_me = True
        if guid:
            tmp_restore = self.get_by_restore_guid(guid)
            if tmp_restore:
                self.name = guid
        else:
            tmp_guid = self.create_restore()
            if tmp_guid:
                self.name = tmp_guid
#        return "5f1e39d373d24db49ead9602e6754c68";
#        response = self.WebService.RestoreTest(username=self.username,pw=self.password,number=num_beams)
#        if response != "fail":
#            self.create_restore(response) #creates restore record in sqlite db with status waiting path
#            self.restores[response] = False  #False means no downloader exist for this restore
#            self.name = response
#            return response
#        else:
#            return False

    def run(self):
        if self.name == False:
            print "Will not start a restore: have no name"
            return self.name
        print "Will run the restore."
        self.update()
        print self.values


        if not self.downloader:
            if self.values['dl_tries'] < downloader_numofretries:
                self.getLocation()
            else:
                return False
        else:
            self.update_status({'dl_status':self.downloader.status})

            if self.downloader.status.split(":")[0] == "Finished":
                return False
            elif self.values['dl_tries'] < downloader_numofretries and\
            self.downloader.status.split(":") == "Failed":
                self.start_downloader()
            elif self.downloader.status.split(":")[0] != "Downloading":
                return False

        return self.name

    def create_restore(self):
        response = self.WebService.RestoreTest(username=self.username,pw=self.password,number=self.num_beams)
        if response != "fail":
            self.name = response
            if self.get_by_restore_guid(self.name) != None:
                print "The record with GUID = '%s' allready exists" % (self.name)
            else:
                insert_query = "INSERT INTO restores (guid, dl_status, dl_tries, created_at) VALUES ('%s','%s', %u, '%s')" % \
                                    (self.name, 'waiting_path', 0, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print insert_query
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(insert_query)
                db_conn.commit()
                db_conn.close()
                self.update()
                return response
        else:
            return False
        
    def getLocation(self):
        #self.my_logger.info("Requesting Location for: "+ self.name)
        response = self.WebService.LocationTest(username=self.username,pw=self.password, guid=self.name)
        if response == "done":
          if not self.downloader:
            print "Creating Downloader for restore: "+ self.name
            self.start_downloader()
        else:
            print "File not ready for: "+ self.name;
            #self.my_logger.info("File not ready for: "+ self.name)
    
    def start_downloader(self):
        self.downloader=downloader(self.name,self.db_name)
        self.update_status({'dl_status':self.downloader.status})
        self.inc_tries()
        self.downloader.start()

    def downloading(self,file_name):
        if self.downloader:
            return True
        else:
            return False

    def update_status(self,named_list):
        update_values = []
        for column, value in named_list.items():
            if type(value) == str:
                update_values.append(column+"='"+value.replace("'","").replace('"',"")+"'")
            elif value == None:
                update_values.append(column+"= NULL")
            else:
                update_values.append(column+"="+str(value))
             
        query = "UPDATE restores SET %s WHERE guid = '%s' " % (", ".join(update_values),self.name)
        done = False
        while not done:
            try:
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(query)
                db_conn.commit()
                db_conn.close()
                done = True
            except Exception as e:
                print "DB error: "+ str(e)
                done = False

    def inc_tries(self):
        done = False
        while not done:
            try:
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                sel_query = "UPDATE restores SET dl_tries = dl_tries + 1 WHERE guid = '%s'" % (self.name)
                db_cur.execute(sel_query)
                db_conn.commit()
                db_conn.close()
                done = True
            except Exception as e:
                done = False

    def update(self):
        self.values = self.get_by_restore_guid(self.name)

    def get_by_restore_guid(self, guid):
        done = False

        while not done:
            try:
                sel_query = "SELECT * FROM restores WHERE guid = '%s' LIMIT 1" % (guid)
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(sel_query)
                row = db_cur.fetchone()
                db_conn.close()
                done= True
                return row
            except Exception as e:
                print "DB error: "+str(e)
                done = False

    def dump_db(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();

        query = "SELECT * FROM restores"
        db_cur.execute(query);
        for row in self.db_cur:
            print row
        db_conn.close()
        print ""
        print ""



class downloader(Thread):

    def __init__(self,restore_dir ,db_name):
#        self.local_drive_avail()
#        exit("end")
        print "DL INIT: "+ restore_dir
        Thread.__init__(self)
        self.block = {}
        self.block['size'] = 0
        self.block['time'] = 0
        self.restore_dir = restore_dir
        self.file_name = None # os.path.basename(file_path)
        self.file_size = 0
        self.download = False
        self.ftp = None
        self.status = None
        self.file_dir = None
        self.start_time = 0
        self.end_time = 0
        self.db_name =db_name
#        self.db_conn = sqlite3.connect(db_name,timeout=1);
#        self.db_conn.row_factory = sqlite3.Row
#        self.db_cur = self.db_conn.cursor();
        
        
        try:
            self.ftp = ftpslib.FTP_TLS()
            self.ftp.connect('arecibo.tc.cornell.edu',31001)
            self.ftp.auth_tls()
            self.ftp.set_pasv(1)
        except Exception as e:
            #self.update_status({'dl_status':"Failed: '"+ self.file_name +"' -- "+ str(e)})
            self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
        
#        if not os.path.exists(os.path.join(rawdata_directory,self.file_name)):
        try:
            print self.ftp.login('palfadata','NAIC305m')
            try:
                self.download = True
                print self.restore_dir
                print self.ftp.cwd(self.restore_dir)
                self.file_name = self.ftp.nlst()[0]
                print "Filename: "+ self.file_name
                self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
                self.status = 'New'
                #self.update_status({'dl_status':'New','filename':self.file_name})
            except Exception as e:
                if not self.status:
                    self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
        except Exception as e:
            if not self.status:
                self.status = "Failed: Login failed '"+ str(self.file_name) +"' -- "+ str(e)
#        else:
#            if not self.status:
#                self.status = "Failed: File '"+ self.file_name +"' already exists."
        self.total_size_got = 0
        self.file_size = 0
        
    def run(self):
        if self.download:
            if self.enough_space():
                try:
                    self.start_time = time.time()
                    self.file_size = self.ftp.size(self.file_name)
                    if self.file_size == 0:
                        raise Exception("File size 0")
                    self.ftp.sendcmd("TYPE I")
                    retr_response = str(self.ftp.retrbinary("RETR "+self.file_name, self.write))
                    self.end_time = time.time()
                    time_took = self.end_time - self.start_time
                    
                    print "Finished: '"+ self.file_name +"' "\
                    +str(self.total_size_got)+" bytes -- Completed in: "+\
                    self.prntime(time_took)

                    self.finished("Finished: '"+ self.file_name +"' "\
                    +str(self.total_size_got)+" bytes -- Completed in: "+\
                    self.prntime(time_took))
                except Exception as e:
                    self.finished('Failed: '+str(e))
            else:
                self.finished('Failed: Not Enough Space to save the remote file.')
        
    def get_file_size(self):
        self.file_size = self.ftp.size(self.file_name)
        return self.file_size

    def inc_tries(self):
        done = False
        while not done:
            try:
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                sel_query = "UPDATE restores SET dl_tries = dl_tries + 1 WHERE guid = '%s'" % (self.name)
                db_cur.execute(sel_query)
                db_conn.commit()
                db_conn.close()
                done = True
            except Exception as e:
                done = False

    def update_status(self, named_list):
        done = False
        while not done:
            try:
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                update_values = []
                import re
                for column, value in named_list.items():
                    if type(value) == str:
                        update_values.append(column+"='"+value.replace("'","").replace('"',"")+"'")
                    elif value == None:
                        update_values.append(column+"= NULL")
                    else:
                        update_values.append(column+"="+str(value))
                query = "UPDATE restores SET %s WHERE guid = '%s' " % (", ".join(update_values),self.restore_dir)
                db_cur.execute(query)
                db_conn.commit()
                db_conn.close()
                done = True
            except Exception as e:
                done = False


    def write(self, block):
        self.total_size_got += len(block)
        self.speed = int(((float(self.total_size_got) - float(self.block['size'\
        ])) / float( time.time() -self.block['time'] ))/1024)
        self.block['time'] = time.time()
        self.block['size'] = self.total_size_got
        #self.update_status({'dl_status': 'Downloading: '+str(self.total_size_got)+" -- "\
        #+ str(int( float(self.total_size_got) / float(self.file_size) * 100 )) +"% -- "\
        #+str(self.speed)+" Kb/s"})
        self.status = 'Downloading: '+str(self.total_size_got)+" -- "\
        + str(int( float(self.total_size_got) / float(self.file_size) * 100 )) +"% -- "\
        +str(self.speed)+" Kb/s"
        self.file.write(block)

    def finished(self,message):
        #print "Closing File: "+self.file_name
        self.status = message
        if self.file:
            self.file.close()
        self.ftp.close()

    def status(self):
        print "File: "+self.file_name
        print "Downloaded: "+ str(self.total_size_got)
        print ""

    def get_working_dir_size(self):
        output = os.popen('du -bs '+rawdata_directory,"r")
        size = output.readline().split('	')[0]
        return int(size)

    def enough_space(self):
        return True
        working_size = self.get_working_dir_size()
        remote_file_size = self.get_file_size()
        if downloader_space_to_use:
            if downloader_space_to_use > (working_size + remote_file_size):
                return True
            else:
                return False
        else:
            return True

    def local_drive_avail(self):
        pipe = os.popen('df --block-size=1')
        print pipe.readline()
        search = re.search(' (\d+) (\d+) (\d+)',pipe.readline())
        print search.group(0)
        total_bytes = int(search.group(1))
        used_bytes = int( search.group(2))
        avail_bytes = int(search.group(3))
        print str(total_bytes - used_bytes)

    def prntime(self,s):
        m,s=divmod(s,60)
        h,m=divmod(m,60)
        d,h=divmod(h,24)
        return_string = ""
        
        if d > 0:
            return_string = str(int(d))+" days "+ str(int(h)) +" hours "+\
            str(int(m)) +" minutes "+ str(int(s)) +" seconds."
        elif d <= 0 and h > 0:
            return_string = str(int(h)) +" hours "+str(int(m)) +" minutes "+\
            str(int(s)) +" seconds."
        elif d <= 0 and h <= 0 and m > 0:
            return_string = str(int(m)) +" minutes "+ str(int(s)) +" seconds."
        else:
            return_string = str(int(s)) +" seconds."

        return return_string

#controller = DownloadModule()
#controller.run()

print "Init restore"
res = restore("sqlite3db",1,"61f48867a7404047877284d27af39ff6")
print "restore initialized"
dl = True
while dl:
    dl = res.run()
    print "Run: "+ str(dl)
    print res.downloader
    sleep(1)

