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
    downloader_numofrestores,downloader_api_username, downloader_numofretries, \
    bgs_db_file_path, bgs_screen_output

from OutStream import OutStream as OutStream
dlm_cout = OutStream("Download Module","downloader.log",bgs_screen_output)
dl_cout = OutStream("Download Module: d/l thread","downloader.log",bgs_screen_output)

class DownloadModule:

    def __init__(self):
        #self.my_logger.info('Initializing.')
        dlm_cout.outs('Initializing Module')
        self.username = downloader_api_username
        self.password = downloader_api_password
        self.db_name = bgs_db_file_path
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
                sleep(3)
            for res in self.restores[:]:
                if not res.run():
                    dlm_cout.outs(res.name +'Could not run the restore...removing. Files:'+ ", ".join(res.files.keys()) )
                    self.restores.remove(res)
                else:
                    dlm_cout.outs(res.name +': Running with files: '+ ", ".join(res.files.keys()) )
                dlm_cout.outs("Number of restores: "+ str(len(self.restores)))
                sleep(3)
            
    def recover(self):
        dlm_cout.outs("Starting recovering process.")        
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        rec_query = "SELECT * FROM restores WHERE dl_status NOT LIKE 'Finished:%' AND dl_status NOT LIKE 'Processed'";
        db_cur.execute(rec_query)
        for res_row in db_cur:
            self.restores.append(restore(db_name=self.db_name,num_beams=1,guid=res_row['guid']))
        db_conn.close()

        
    def have_space(self):
        
        folder_size = 0
        for (path, dirs, files) in os.walk(downloader_temp):
          for file in files:
            try:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
            except Exception, e:
                dlm_cout.outs('There was an error while getting the file size: %s   Exception: %s' % (filename,str(e)) )
        
            
        if folder_size < downloader_space_to_use:
            dlm_cout.outs(str(folder_size) +" <? "+ str(downloader_space_to_use))
            dlm_cout.outs("Enough Space")
            return True
        else:
            dlm_cout.outs("Not Enough Space")
            return False


    def can_request_more(self):
        if len(self.restores) >= downloader_numofrestores:
            dlm_cout.outs("Cannot have more than "+ str(downloader_numofrestores) +" at a time.")
            return False
        else:
            dlm_cout.outs("Will request more files")
            dlm_cout.outs("Can request more restores.")
            return True

    def get_by_restore_guid(self, guid):        
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        sel_query = "SELECT * FROM restores WHERE guid = '%s' LIMIT 1" % (guid)
        db_cur.execute(sel_query)
        record = db_cur.fetchone()
        db_conn.close()
        return record

    def dump_db(self):        
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "SELECT * FROM restores"
        db_cur.execute(query);
        for row in self.db_cur:
            print row
        print ""
        print ""
        db_conn.close()

class restore:

    def __init__(self,db_name, num_beams,guid=False):
        self.values = None
        self.num_beams = num_beams
        self.db_name = db_name
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

    #TODO: Refactor this function and the helper functions
    def run(self):
        if self.name == False:
            dlm_cout.outs("Will not start a restore: have no name", OutStream.WARNING)
            return self.name
        dlm_cout.outs("Will run the restore.")
        self.update_dl_status()
        self.update()
        print self.values
        
        if self.values['dl_status'].split(":")[0] == "Finished":
            #TODO: remove in refactored
            self.update_dl_status()
            return False
        elif self.values['dl_status'].split(":")[0] == "waiting_path":
            if self.getLocation():
                self.get_files()
                self.create_dl_entries()
                self.update_status({'dl_status': "Download Ready:_"})
                #TODO: remove in refactored
                self.update_dl_status()
        elif self.values['dl_status'].split(":")[0] == "Download Ready":
            if self.is_finished():
                return False
            else:
                dlm_cout.outs("Starting Downloaders for: %s" % self.name)
                if self.files == dict():
                    self.get_files()
                self.create_dl_entries()
                if not self.start_downloader():
                    return False

        self.update_dl_status()
        self.update()
        return True

    def create_restore(self):
        dlm_cout.outs("Requesting Restore")
        response = self.WebService.Restore(username=self.username,pw=self.password,number=self.num_beams,bits=4,fileType="wapp")
        if response != "fail":
            self.name = response
            if self.get_by_restore_guid(self.name) != None:
                dlm_cout.outs("The record with GUID = '%s' allready exists" % (self.name))
            else:
                dlm_cout.outs("Creating DB Entry for GUID = %s" % (response))
                insert_query = "INSERT INTO restores (guid, dl_status, dl_tries, created_at) VALUES ('%s','%s', %u, '%s')" % \
                                    (self.name, 'waiting_path', 0, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                dlm_cout.outs(insert_query, OutStream.DEBUG)
                db_conn = sqlite3.connect(self.db_name);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(insert_query)
                db_conn.commit()
                db_conn.close()
                self.update()
                return response
        else:
            dlm_cout.outs("Failed to receive proper GUID", OutStream.WARNING)
            return False
        
    def getLocation(self):
        #self.my_logger.info("Requesting Location for: "+ self.name)
        response = self.WebService.Location(username=self.username,pw=self.password, guid=self.name)
        if response == "done":
            dlm_cout.outs("File ready for restore: %s" % (self.name))
            return True
        else:
            dlm_cout.outs("File not ready for: %s " % (self.name));
            return False
            
    
    def get_files(self):
        dlm_cout.outs("Getting files list for restore: %s" % (self.name))
        connected = False
        logged_in = False
        cwd = False
        list_cmd = False
        got_all_files_size = False

        no_connection = True
        while no_connection:
            try:
                ftp = ftpslib.FTP_TLS()
                ftp.connect('arecibo.tc.cornell.edu',31001)
                ftp.auth_tls()
                ftp.set_pasv(1)
                connected = True

                login_response = ftp.login('palfadata','NAIC305m')
                logged_in = True
                if login_response != "230 User logged in.":
                    dlm_cout.outs(self.name +" Could not login with user: palfadata  password: NAIC305m")
                    return False

                cwd_response = ftp.cwd(self.name)
                cwd = True
                if cwd_response != "250 CWD command successful.":
                    dlm_cout.outs(self.name+" Restore Directory not found", OutStream.WARNING)
                    return False

                files_in_res_dir = ftp.nlst()
                list_cmd = True

                for file in files_in_res_dir:
                    file_size = ftp.size(file)
                    dlm_cout.outs(self.name +" got file size for "+ file)
                    self.size += file_size
                    self.files[file] = file_size
                got_all_files_size = True

                no_connection = False
            except Exception, e:
                dlm_cout.outs(self.name +" FTP-Connection Error: "+ str(e) +"Wating for retry...2 seconds", OutStream.WARNING)
                dlm_cout.outs(self.name +" FTP-Connection Managed to Connect: "+ str(connected), OutStream.WARNING)
                dlm_cout.outs(self.name +" FTP-Connection Managed to Login: "+ str(logged_in), OutStream.WARNING)
                dlm_cout.outs(self.name +" FTP-Connection Managed to CWD: "+ str(cwd), OutStream.WARNING)
                dlm_cout.outs(self.name +" FTP-Connection Managed to List-Cmd: "+ str(list_cmd), OutStream.WARNING)
                dlm_cout.outs(self.name +" FTP-Connection Managed to Get-All-Files'-Size: "+ str(got_all_files_size), OutStream.WARNING)
                sleep(2)
            
        ftp.close()
    
    def create_dl_entries(self):
        dlm_cout.outs("Creating download entries for: %s." % (self.name))
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        
        for filename,filesize in self.files.items():
            sel_query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s'" % (self.name,filename)
            db_cur.execute(sel_query)
            if len(db_cur.fetchall()) <= 0:
                query = "INSERT INTO restore_downloads (guid,filename,num_tries,status) VALUES ('%s','%s',%u,'%s')" % (self.name,filename,0,'New')
                db_cur.execute(query)
                db_conn.commit()
    #TODO: Refactor function and helpers
    def start_downloader(self):
        started_atleast_one = False
        for filename,filesize in self.files.items():
            if not filename in self.downloaders:
                if downloader_numofretries > int(self.get_tries(filename)) and not self.have_finished(filename):
                    dlm_cout.outs("Starting download of file %s for %s" % (filename, self.name))
                    self.downloaders[filename] = downloader(self.name,filename)
                    self.inc_tries(filename)
                    self.downloaders[filename].start()
                else:
                    dlm_cout.outs("========= "+filename+" ========")
                    dlm_cout.outs("downloader_numofretries > int(self.get_tries(filename)):" +str(downloader_numofretries > int(self.get_tries(filename))) )
                    dlm_cout.outs("int(self.get_tries(filename)):" +str(int(self.get_tries(filename))))
                    dlm_cout.outs("downloader_numofretries:" +str(downloader_numofretries))
                    dlm_cout.outs("not self.have_finished(filename): "+ str(not self.have_finished(filename)))
                    dlm_cout.outs(self.name +" Maximum retries reached for: "+ filename)
            if filename in self.downloaders:
                    if self.downloaders[filename].is_alive() == False:
                        dlm_cout.outs("Thread downloading %s for %s died..." % (filename, self.name), OutStream.WARNING)
                        if downloader_numofretries > int(self.get_tries(filename)) and not self.have_finished(filename):
                            self.downloaders[filename] = downloader(self.name,filename)
                            self.inc_tries(filename)
                            self.downloaders[filename].start()
                            started_atleast_one = True
                            dlm_cout.outs("Will restart the thread downloading %s for %s" % (filename, self.name))
                        else:
                            dlm_cout.outs("Will NOT restart the thread downloading %s for %s" % (filename, self.name))
                    else:
                        started_atleast_one = True
            
        return started_atleast_one
                    
                
    def have_finished(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s' LIMIT 1" % (self.name,filename)
        db_cur.execute(query)
        row = db_cur.fetchone()
        db_conn.close()
        if row['status'].split(":")[0] == 'Finished':
            dlm_cout.outs("Finished Downloading %s for %s " % (filename, self.name))
            return True
        else:
            dlm_cout.outs("Finished Downloading %s for %s " % (filename, self.name))
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
        if fin_count == all_count:
            self.update_status({"dl_status": "Finished:_"})
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
                dlm_cout.outs("DataBase error: %s" % str(e), OutStream.ERROR)
                done = False
    
    def update_dl_status(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();

        for filename,dl_instance in self.downloaders.items():
            query = "UPDATE restore_downloads SET status = '%s' WHERE guid = '%s' and filename = '%s'" % (dl_instance.status.replace("'","").replace('"',''),self.name,filename)
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
                dlm_cout.outs("DataBase Error in get_by_restore_guid(self,guid): %s" % str(e), OutStream.ERROR)
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
        dl_cout.outs("Initializing Downloader for: %s" % restore_dir)
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
        
        not_logged_in = True
        while not_logged_in:
            try:
                self.ftp = ftpslib.FTP_TLS()
                self.ftp.connect('arecibo.tc.cornell.edu',31001)
                self.ftp.auth_tls()
                self.ftp.set_pasv(1)
                login_response = self.ftp.login('palfadata','NAIC305m')
                if login_response != "230 User logged in.":
                    dl_cout.outs("Could not login with user: palfadata  password: NAIC305m", OutStream.ERROR)
                    self.status = "Failed: Login failed '"+ str(self.file_name) +"' -- "
                self.download = True
                cwd_response = self.ftp.cwd(self.restore_dir)
                if cwd_response != "250 CWD command successful.":
                    dl_cout.outs("Restore Directory not found", OutStream.ERROR)
                    self.status = "Failed: Directory change failed '"+ str(self.file_name) +"' -- "
                not_logged_in = False
            except Exception , e:
                #self.update_status({'dl_status':"Failed: '"+ self.file_name +"' -- "+ str(e)})
                #self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
                dl_cout.outs("Could not connect to host. Waiting 1 sec: %s " % (self.file_name) )
                sleep(1)
        
        self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
        self.status = 'New'

        if not self.status:
            self.status = "Failed: '"+ self.file_name +"' -- "#+ str(e)

        if not self.status:
            self.status = "Failed: Login failed '"+ str(self.file_name) +"' -- "#+ str(e)

        self.total_size_got = 0
        self.file_size = 0
        
    def run(self):
        dl_cout.outs("Starting Download of %s for %s " % (self.file_name, self.restore_dir) )
        try:
            self.start_time = time.time()
            self.file_size = self.ftp.size(self.file_name)
            if self.file_size == 0:
                dl_cout.outs("%s size is 0", OutStream.ERROR)
                raise Exception("File size 0")
            self.ftp.sendcmd("TYPE I")
            retr_response = str(self.ftp.retrbinary("RETR "+self.file_name, self.write))
            self.end_time = time.time()
            time_took = self.end_time - self.start_time
            
            self.finished("Finished: '"+ self.file_name +"' "\
            +str(self.total_size_got)+" bytes -- Completed in: "+\
            self.prntime(time_took))
            
        except Exception , e:
            self.finished('Failed: in Downloader.run() '+str(e))
        
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
        dl_cout.outs(message)
        self.status = message
        if self.file:
            self.file.close()
        self.ftp.close()

    def status(self):
        #TODO: print download status
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

