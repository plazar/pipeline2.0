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
        self.downloaders = []
        self.restores = []

#    def run(self):
#        while True:
#            if self.have_space():
#                if self.can_request_more():
#                    response = self.getRestore()
#                    print "Requested Restore:"+ response
#                    self.my_logger.info("Requested Restore")
#                    if response != "fail":
#                        #self.downloaders.append(downloader())
#                        print "Restore Response: "+ response
#                        #self.my_logger.info("Restore Response: "+ response)
#                        self.create_restore(response)
#                        self.restores.append(response)
#                    else:
#                        self.my_logger.warning("Restore Response: "+ response)
#
#                for restore in self.restores:
#                    self.getLocation(restore)
#
#                if self.can_download_more():
#                    for download in self.downloaders:
#                        if download.status == 'New':
#                            download.start()
#                            self.my_logger.info("Starting Download: "+download.file_name)
#            else:
#                if not self.have_space():
#                    self.my_logger.warning\
#                    ("Not enough space specified in config file to store the file.")
#
#            for download in self.downloaders[:]:
#                print download.status
#                if download.status.split(':')[0] == "Downloaded" or \
#                download.status.split(':')[0] == 'Failed':
#                    self.my_logger.info(download.status)
#                    self.downloaders.remove(download)
#            sleep(1)
#            print ""
#
#
#
#    def getLocation(self,restore):
#        self.my_logger.info("Requesting Location for: "+ restore)
#        response = self.WebService.LocationTest(username=self.username,pw=self.password, guid=restore)
#        if response != "incomplete" and response  != "Error":
##           if not self.downloading(response):
##               self.downloaders.append(downloader(response))
##               self.restores.remove(restore)
#
#          if not self.downloading(restore):
#            print "Creating Downloader: "+ restore
#            self.downloaders.append(downloader(restore,self.db_name))
#            self.restores.remove(restore)
#          else:
#            self.restores.remove(restore)
#
#
#
#
#
#    def restore(self):
#        return "5f1e39d373d24db49ead9602e6754c68";
#        return self.WebService.RestoreTest(username=self.username,pw=self.password,number=1)
#
#    def have_space(self):
#        return True
#
#    def can_download_more(self):
#        downloading_count = 0
#        for download in self.downloaders[:]:
#                if download.status.split(':')[0] == "Downloading":
#                    downloading_count += 1
#        if downloading_count >= downloader_numofdownloads:
#            print "Cannot download more than: "+ str(downloader_numofdownloads) +" at a time."
#            return False
#        else:
#            print "Will download."
#            return True
#
#    def can_request_more(self):
#        if len(self.restores) >= downloader_numofrestores:
#            print "Cannot have more than "+ str(downloader_numofrestores) +" at a time."
#            return False
#        else:
#            print "Will request more files"
#            return True
#
#    def get_by_restore_guid(self, guid):
#        sel_query = "SELECT * FROM restores WHERE guid = '%s' LIMIT 1" % (guid)
#        self.db_cur.execute(sel_query)
#        return self.db_cur.fetchone()

class restore:

    def __init__(self,db_name, num_beams):
        self.db_name = db_name
        self.db_conn = sqlite3.connect("sqlite3db");
        self.db_conn.row_factory = sqlite3.Row
        self.db_cur = self.db_conn.cursor();
        self.downloader = False
        self.WebService =  Client(downloader_api_service_url).service
        self.username = downloader_api_username
        self.password = downloader_api_password
        self.name = "5f1e39d373d24db49ead9602e6754c68"
#        return "5f1e39d373d24db49ead9602e6754c68";
#        response = self.WebService.RestoreTest(username=self.username,pw=self.password,number=num_beams)
#        if response != "fail":
#            self.create_restore(response) #creates restore record in sqlite db with status waiting path
#            self.restores[response] = False  #False means no downloader exist for this restore
#            self.name = response
#            return response
#        else:
#            return False

    def create_restore(self, guid):
        if self.get_by_restore_guid(guid) != None:
            print "The record with GUID = '%s' allready exists" % (guid)
        else:
            insert_query = "INSERT INTO restores (guid, dl_status, dl_tries, created_at) VALUES ('%s','%s', %u, '%s')" % \
                            (guid, 'waiting_path', 0, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print insert_query
            self.db_cur.execute(insert_query)
            self.db_conn.commit()

    def getLocation(self):
        #self.my_logger.info("Requesting Location for: "+ self.name)
        response = self.WebService.LocationTest(username=self.username,pw=self.password, guid=self.name)
        if response == "done":
          if not self.downloader:
            print "Creating Downloader for restore: "+ self.name
            self.downloader=downloader(self.name,self.db_name)
            self.downloader.start()
        else:
            print "File not ready for: "+ self.name;
            #self.my_logger.info("File not ready for: "+ self.name)

    def downloading(self,file_name):
        if self.downloader:
            return True
        else:
            return False

    def update_status(self,named_list):
        update_values = []
        for column, value in named_list.items():
            if type(value) == str:
                update_values.append(column+"='"+value+"'")
            elif value == None:
                update_values.append(column+"= NULL")
            else:
                update_values.append(column+"="+str(value))
             
        query = "UPDATE restores SET %s WHERE guid = '%s' " % (", ".join(update_values),self.name)
        self.db_cur.execute(query)
        self.db_conn.commit()

    def dump_db(self):
        query = "SELECT * FROM restores"
        self.db_cur.execute(query);
        for row in self.db_cur:
            print row
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
        self.db_conn = sqlite3.connect(db_name);
        self.db_conn.row_factory = sqlite3.Row
        self.db_cur = self.db_conn.cursor();
        
        
        try:
            self.ftp = ftpslib.FTP_TLS()
            self.ftp.connect('arecibo.tc.cornell.edu',31001)
            self.ftp.auth_tls()
            self.ftp.set_pasv(1)
        except Exception as e:
            self.update_status({'dl_status':"Failed: '"+ self.file_name +"' -- "+ str(e)})
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
                self.update_status({'dl_status':'New'})
            except Exception as e:
                if not self.status:
                    self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
        except Exception as e:
            if not self.status:
                self.status = "Failed: Login failed '"+ self.file_name +"' -- "+ str(e)
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
                    shutil.move(os.path.join(downloader_temp,self.file_name),rawdata_directory)
                    self.end_time = time.time()
                    time_took = self.end_time - self.start_time
                    self.finished("Finished: '"+ self.file_name +"' "\
                    +str(self.total_size_got)+" bytes -- Completed in: "+\
                    self.prntime(time_took))
                except Exception as e:
                    self.update_status({'dl_status':'Failed: '+str(e)})
                    self.finished('Failed: '+str(e))
            else:
                self.finished('Failed: Not Enough Space to save the remote file.')
        
    def get_file_size(self):
        self.file_size = self.ftp.size(self.file_name)
        return self.file_size

    def update_status(self, named_list):
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

    def write(self, block):
        self.total_size_got += len(block)
        self.speed = int(((float(self.total_size_got) - float(self.block['size'\
        ])) / float( time.time() -self.block['time'] ))/1024)
        self.block['time'] = time.time()
        self.block['size'] = self.total_size_got
        self.update_status({'dl_status': 'Downloading: '+str(self.total_size_got)+" -- "\
        + str(int( float(self.total_size_got) / float(self.file_size) * 100 )) +"% -- "\
        +str(self.speed)+" Kb/s"})
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

res = restore("sqlite3db",1)
res.update_status({'dl_status':None,'dl_tries':6})
res.dump_db()
res.getLocation()
res.dump_db()

while True:
    res.dump_db()
    sleep(1);
