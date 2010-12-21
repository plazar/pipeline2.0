import os.path
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
            while self.can_request_more() and self.have_space():
                #self.restores.append(time.time())
                self.restores.append(restore(db_name=self.db_name,num_beams=1))
                sleep(2)
            for res in self.restores[:]:
                if not res.run():
                    print "Could not run the restore...deleting"
                    self.restores.remove(res)
                else:
                    print res.files

                print "Number of restores: "+ str(len(self.restores))


                sleep(2)
            
    def recover(self):
        rec_query = "SELECT * FROM restores WHERE dl_status NOT LIKE 'Finished:%'";
        self.db_cur.execute(rec_query)
        for res_row in self.db_cur:
            self.restores.append(restore(db_name=self.db_name,num_beams=1,guid=res_row['guid']))

        
    def have_space(self):
        folder_size = 0
        for (path, dirs, files) in os.walk(downloader_temp):
          for file in files:
            filename = os.path.join(path, file)
            folder_size += os.path.getsize(filename)
        if folder_size < downloader_space_to_use:
            print "Enough Space"
            return True
        else:
            print "Not Enough Space"
            return False


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
        self.downloaders = dict()
        self.WebService =  Client(downloader_api_service_url).service
        self.username = downloader_api_username
        self.password = downloader_api_password
        self.remove_me = True

        self.files = dict()
        self.size = 0
        if guid:
            self.name = guid
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
        
        if self.values['dl_status'].split(":")[0] == "Finished":
            return False
        elif self.values['dl_status'].split(":")[0] == "waiting_path":
            if self.getLocation():
                self.get_files()
                self.create_dl_entries()
                self.update_status({'dl_status': "Download Ready:_"})
        elif self.values['dl_status'].split(":")[0] == "Download Ready":
            if self.is_finished():
                return False
            else:
                print "Starting Downloaders for: "+self.name
                self.get_files()
                self.create_dl_entries()
                self.start_downloader()
                return True

        self.update()
        return True

    def create_restore(self):
        response = self.WebService.Restore(username=self.username,pw=self.password,number=self.num_beams,bits=4,fileType="wapp")
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
        response = self.WebService.Location(username=self.username,pw=self.password, guid=self.name)
        if response == "done":
            return True
        else:
            print "File not ready for: "+ self.name;
            return False
            
    
    def get_files(self):
        ftp = ftpslib.FTP_TLS()
        ftp.connect('arecibo.tc.cornell.edu',31001)
        ftp.auth_tls()
        ftp.set_pasv(1)
        print ftp.login('palfadata','NAIC305m')
        print ftp.cwd(self.name)
        for file in ftp.nlst():
            file_size = ftp.size(file)
            self.size += file_size
            self.files[file] = file_size
        ftp.close()
    
    def create_dl_entries(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        
        for filename,filesize in self.files.items():
            sel_query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s'" % (self.name,filename)
            print sel_query
            db_cur.execute(sel_query)
            if len(db_cur.fetchall()) <= 0:
                query = "INSERT INTO restore_downloads (guid,filename,num_tries,status) VALUES ('%s','%s',%u,'%s')" % (self.name,filename,0,'New')
                print query
                db_cur.execute(query)
                db_conn.commit()
    
    def start_downloader(self):
        for filename,filesize in self.files.items():
            if not filename in self.downloaders:
                if downloader_numofretries > int(self.get_tries(filename)) and not self.have_finished(filename):
                    self.downloaders[filename] = downloader(self.name,filename)
                    self.inc_tries(filename)
                    self.downloaders[filename].start()
                
    def have_finished(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s' LIMIT 1" % (self.name,filename)
        db_cur.execute(query)
        row = db_cur.fetchone()
        db_conn.close()
        if row['status'].split(":")[0] == 'Finished':
            print "Finished: "+ str(filename)
            return True
        else:
            print "Not Finished: "+ str(filename)
            return False

    def is_finished(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        all_query = "SELECT * FROM restore_downloads WHERE guid = '%s'" % (self.name)
        db_cur.execute(all_query)
        all_count = len(db_cur.fetchall())
        
        fin_query = "SELECT * FROM restore_downloads WHERE guid = '"+self.name+"' AND status LIKE 'Finished:%'"
        db_cur.execute(fin_query)
        fin_count = len(db_cur.fetchall())
        db_conn.close()
        return (fin_count == all_count)

    def get_tries(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s' LIMIT 1" % (self.name,filename)
        db_cur.execute(query)
        row = db_cur.fetchone()
        db_conn.close()
        return row['num_tries']

    def inc_tries(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "UPDATE restore_downloads SET num_tries = num_tries + 1 WHERE guid = '%s' AND filename = '%s'" % (self.name,filename)
        db_cur.execute(query)
        db_conn.commit()
        db_conn.close()
        
    def update_or_insert_dl(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        
        query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s'" % (self.name,filename)
        print query
        db_cur.execute(query)
        db_conn.commit()
        if len(db_cur.fetchall()) > 0:
            query = "UPDATE restore_downloads SET num_tries = num_tries + 1, status = 'New' WHERE guid = '%s' AND filename = '%s'" % (self.name,filename)
        else:
            query = "INSERT INTO restore_downloads (guid,filename,num_tries,status) VALUES ('%s','%s',%u,'%s')" % (self.name,filename,1,'New')
        print query
        db_cur.execute(query)
        db_conn.commit()
        db_conn.close()
#        self.downloader=downloader(self.name,self.db_name)
#        self.update_status({'dl_status':self.downloader.status})
#        if self.downloader.file_name:
#            self.update_status({'filename':self.downloader.file_name})
#        self.inc_tries()
#        self.downloader.start()

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
            except Exception, e:
                print "DB error: "+ str(e)
                done = False
    
    def update_dl_status(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();

        for filename,dl_instance in self.downloaders.items():
            query = "UPDATE restore_downloads SET status = '%s' WHERE guid = '%s' and filename = '%s'" % (instance.status.replace("'","").replace('"',''),self.name,filename)
            db_cur.execute(query)
            db_conn.commit()
        db_conn.close()

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
            except Exception , e:
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

    def __init__(self,restore_dir ,filename):
#        self.local_drive_avail()
#        exit("end")
        print "DL INIT: "+ restore_dir
        Thread.__init__(self)
        self.block = {}
        self.block['size'] = 0
        self.block['time'] = 0
        self.restore_dir = restore_dir
        self.file_name = filename # os.path.basename(file_path)
        self.file_size = 0
        self.download = False
        self.ftp = None
        self.status = None
        self.file_dir = None
        self.start_time = 0
        self.end_time = 0
#        self.db_conn = sqlite3.connect(db_name,timeout=1);
#        self.db_conn.row_factory = sqlite3.Row
#        self.db_cur = self.db_conn.cursor();
        
        
        try:
            self.ftp = ftpslib.FTP_TLS()
            self.ftp.connect('arecibo.tc.cornell.edu',31001)
            self.ftp.auth_tls()
            self.ftp.set_pasv(1)
        except Exception , e:
            #self.update_status({'dl_status':"Failed: '"+ self.file_name +"' -- "+ str(e)})
            self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
        
#        if not os.path.exists(os.path.join(rawdata_directory,self.file_name)):
#        try:
        print self.ftp.login('palfadata','NAIC305m')
#            try:
        self.download = True
        print self.restore_dir
        print self.ftp.cwd(self.restore_dir)
        print "Filename: "+ self.file_name
        self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
        self.status = 'New'
                #self.update_status({'dl_status':'New','filename':self.file_name})
#            except Exception , e:
        if not self.status:
            self.status = "Failed: '"+ self.file_name +"' -- "#+ str(e)
#        except Exception , e:
        if not self.status:
            self.status = "Failed: Login failed '"+ str(self.file_name) +"' -- "#+ str(e)
#        else:
#            if not self.status:
#                self.status = "Failed: File '"+ self.file_name +"' already exists."
        self.total_size_got = 0
        self.file_size = 0
        
    def run(self):
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
        except Exception , e:
            self.finished('Failed: '+str(e))
        
    def get_file_size(self):
        self.file_size = self.ftp.size(self.file_name)
        return self.file_size

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

controller = DownloadModule()
controller.run()

#print "Init restore"
#res = restore("sqlite3db",1)
#print "restore initialized"
#
#while res.run():
#    res.start_downloader()
#    for dl,instance in res.downloaders.items():
#        print instance.status
#    print "\n\n"
#    sleep(2)

#print res.get_tries("p2030_54638_26229_0067_G67.46-03.13_4.w4bit.fits")
#res.inc_tries("p2030_54638_26229_0067_G67.46-03.13_4.w4bit.fits")
#print res.get_tries("p2030_54638_26229_0067_G67.46-03.13_4.w4bit.fits")
#res.is_finished("p2030_54638_26229_0067_G67.46-03.13_4.w4bit.fits")
#while True:
#    res.start_downloader()
#    for dl,instance in res.downloaders.items():
#        print instance.status
#    print "\n\n"
#    res.update_dl_status()
#    sleep(2)

    
#dl = True
#while dl:
#    dl = res.run()
#    print "Run: "+ str(dl)
#   print res.downloader
#    sleep(1)

