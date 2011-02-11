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
from urllib2 import URLError

from mailer import ErrorMailer

from downloader_config import downloader_api_username\
                            , downloader_api_password\
                            , downloader_temp\
                            , downloader_space_to_use\
                            , downloader_numofrestores\
                            , downloader_api_service_url\
                            , downloader_numofretries\
                            , downloader_numofdownloads
                            
from master_config import bgs_screen_output\
                        , email_on_failures\
                        , email_on_terminal_failures\
                        , delete_rawdata\
                        , bgs_db_file_path\
                        , base_results_directory

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
        self.recover()
        
    def run(self):
        #if can create more restores then request new ones and add them to restores array
        while True:
            running_restores_count = 0
            running_downloaders_count = 0
            if self.can_request_more():
                dlm_cout.outs("Requesting restore")
                tmp_restore = restore(db_name=self.db_name,num_beams=1)
                if tmp_restore.request():
                    self.restores.append(tmp_restore)
            for res in self.restores[:]:
                if not res.run():
                    self.restores.remove(res)
                else:
                    res.status()
                    running_restores_count += 1
                    running_downloaders_count += 1
            dlm_cout.outs("Number of running restores: %u"+ running_restores_count)
            print "\n\n"
            sleep(37)
            
    def recover(self):
        unfinished_requests = self.query("SELECT * FROM requests WHERE status NOT LIKE 'finished'")
        for request in unfinished_requests:
            self.restores.append(restore(db_name=self.db_name,num_beams=1,guid=request['guid']))
        dlm_cout.outs("Recovered: %u restores" % len(self.restores))
        
    def query(self,query_string):
        not_connected = True
        counter = 0 
        while not_connected:
            try:
                db_conn = sqlite3.connect(bgs_db_file_path,timeout=40.0)
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
                if counter > 59:
                    dlm_cout.outs("Couldn't connect to DB retrying in 1 sec.%s" % str(e)) 
                time.sleep(1)
                counter += 1
        return results
    
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
            return True
        else:
            return False

    def can_request_more(self):
        if len(self.restores) >= downloader_numofrestores:
            dlm_cout.outs("Cannot have more than "+ str(downloader_numofrestores) +" at a time.")
            return False
        
        total_size = 0
        for request in self.restores:
            if request.values['size']:
                total_size += int(request.values['size'])
        
        dlm_cout.outs("Total estimated size of currently running restores: %u" % total_size)
        return ((self.get_available_space() - total_size) > 0)

    def get_available_space(self):
        folder_size = 0
        if downloader_temp == "":
            print "Getting filename"
            path_to_size = os.path.dirname(__file__)
        else:
            print "Setting to downloader_temp"
            path_to_size = downloader_temp
        print path_to_size
        for (path, dirs, files) in os.walk(path_to_size):
          for file in files:
            try:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
            except Exception, e:
                dlm_cout.outs('There was an error while getting the file size: %s   Exception: %s' % (file,str(e)))
        return (downloader_space_to_use - folder_size)
    
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
        self.guid = guid
        if self.guid:
            self.update_from_db()

    def run(self):
        """If this doesn't have guid then we request it 
        """
        if self.guid == False:
            return self.request()
        
        self.update_from_db()
        print self.values
        
        if self.values['status'] == "waiting":
            #TODO: remove in refactored
            self.getLocation()
        elif self.values['status'] == "ready":
            if self.is_finished():
                return False
            if self.files == dict():
                self.get_files()
            self.download()
        elif self.values['status'] == "finished" or self.values['status'] == "failed":  
            dlm_cout.outs("Restore: %s is %s" % (self.guid,self.values['status']))
            return False
        return True

    def request(self):
        dlm_cout.outs("Requesting Restore")
        try:
            response = self.WebService.Restore(username=self.username,pw=self.password,number=self.num_beams,bits=4,fileType="wapp")
        except URLError, e:
            dlm_cout.outs("There was a problem requesting the restore. Reason: %s" % str(e))
            return False
        #response = '9818e194a5db4f4d90aa706826d69907'
        if response != "fail":
            self.guid = response
            if self.get_by_guid(self.guid) != list():
                dlm_cout.outs("The record with GUID = '%s' allready exists" % (self.guid))
            else:
                insert_query = "INSERT INTO requests (guid, created_at, updated_at, status, details) VALUES ('%s','%s', '%s', '%s','%s')" % \
                                    (self.guid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), \
                                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'waiting', 'Newly created restore request')
                dlm_cout.outs(insert_query, OutStream.DEBUG)
                self.query(insert_query)
                return response
        else:
            dlm_cout.outs("Failed to receive proper GUID", OutStream.WARNING)
            return False
    
    def is_downloading(self):
        if self.downloaders == dict():
            return False
        else:
            atleast_one = False
            for filename in self.downloaders:
                if self.downloaders[filename].is_alive():
                    atleast_one = True        
        return atleast_one
        
    
    def getLocation(self):
        #self.my_logger.info("Requesting Location for: "+ self.name)
        response = self.WebService.Location(username=self.username,pw=self.password, guid=self.guid)
        if response == "done":
            self.query("UPDATE requests SET status = 'ready' WHERE guid ='%s'" % (self.guid))
            return True
        else:
            return False
            
    def query(self,query_string):
        not_connected = True
        counter=0
        while not_connected:
            try:
                db_conn = sqlite3.connect(bgs_db_file_path,timeout=40.0);
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
                if counter > 59:    
                    dlm_cout.outs("Couldn't connect to DB retrying in 1 sec.: %s" % str(e)) 
                time.sleep(1)
                counter+=1
        return results
                
    def get_files(self):
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
                    #dlm_cout.outs(self.guid +" Could not login with user: palfadata  password: NAIC305m  Response: %s" % login_response)
                    return False

                cwd_response = ftp.cwd(self.guid)
                cwd = True
                if cwd_response != "250 CWD command successful.":
                    #dlm_cout.outs(self.guid+" Restore Directory not found", OutStream.WARNING)
                    return False
                
                files_in_res_dir = ftp.nlst()
                list_cmd = True
                
                for file in files_in_res_dir:
                    if not re.match('.*7\.w4bit\.fits',file):
                        file_size = ftp.size(file)
                        dlm_cout.outs(self.guid +" got file size for "+ file)
                        self.size += file_size
                        self.files[file] = file_size
                    else:
                        dlm_cout.outs(self.guid +" IGNORING: %s" % file)
                        
                got_all_files_size = True
                no_connection = False
            except Exception, e:
                dlm_cout.outs(self.guid +" FTP-Connection Error: "+ str(e) +"Wating for retry...2 seconds", OutStream.WARNING)
                dlm_cout.outs(self.guid +" FTP-Connection Managed to Connect: "+ str(connected), OutStream.WARNING)
                dlm_cout.outs(self.guid +" FTP-Connection Managed to Login: "+ str(logged_in), OutStream.WARNING)
                dlm_cout.outs(self.guid +" FTP-Connection Managed to CWD: "+ str(cwd), OutStream.WARNING)
                dlm_cout.outs(self.guid +" FTP-Connection Managed to List-Cmd: "+ str(list_cmd), OutStream.WARNING)
                dlm_cout.outs(self.guid +" FTP-Connection Managed to Get-All-Files'-Size: "+ str(got_all_files_size), OutStream.WARNING)
                if connected and logged_in and not cwd:
                    self.query("UPDATE requests SET status = 'finished',details='request directory not found',updated_at='%s' WHERE guid='%s'" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.guid))
                    try:
                        notification = ErrorMailer('The restore APi reported the restores to be ready. However the restore directory does not exist on the FTP.\n Restore GUID: %s' % self.guid)
                        notification.send()
                    except Exception,e:
                        pass
                    return False
                sleep(2)
        self.query("UPDATE requests SET size = '%u' WHERE guid='%s'" % (self.size,self.guid))    
        ftp.close()
    
    def create_dl_entries(self):        
        for filename,filesize in self.files.items():            
            dl_check = self.query("SELECT * FROM downloads WHERE request_id=%s AND filename='%s'" % (self.values['id'],filename))            
            if len(dl_check) == 0:
                query = "INSERT INTO downloads (request_id,remote_filename,filename,status,created_at,updated_at,size) VALUES ('%s','%s','%s','%s','%s','%s',%u)"\
                        % (self.values['id'],filename,os.path.join(downloader_temp,filename),'New',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(filesize))
                self.query(query)
            
    #TODO: Refactor function and helpers
    def download(self):
        dl_entries = self.query("SELECT * FROM downloads WHERE request_id = %u" % self.values['id'])
        #if no downloaders are running for this restore
        if self.downloaders == dict():
            #if no downloads entries present for this restore
            if dl_entries == list():
                #created downloads entries for this restore (for all files)
                self.create_dl_entries()
        
        #get downloads entries for this restore (for all files)
        dl_entries = self.query("SELECT * FROM downloads WHERE request_id = %u and status NOT LIKE 'downloaded'" % self.values['id'])
        #for each downloads entry
        for dl_entry in dl_entries:
            #get number of attempts for this downoad
            this_download_attempts_count = len(self.query("SELECT * from download_attempts WHERE download_id = %s" % dl_entry['id']))
            #if downloader is not running for this entry            
            if not dl_entry['remote_filename'] in self.downloaders:
                #if maximum number of attempts is not reached
                if downloader_numofretries > this_download_attempts_count:
                    #create an attempt entry and downloader refering to this attempt by id
                    id = self.query("INSERT INTO download_attempts (download_id,created_at,updated_at) VALUES  ('%s','%s','%s')" % \
                    (dl_entry['id'],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ))
                    #created downloader thread
                    self.downloaders[dl_entry['remote_filename']] = downloader(self.guid,dl_entry['remote_filename'],id)
                    #run downloader thread
                    self.downloaders[dl_entry['remote_filename']].start()
        
        #update download status and remove dead downloaders
        for filename in self.downloaders.keys():
            if not self.downloaders[filename].is_alive():
                if self.downloaders[filename].status == 'failed':
                    self.query("UPDATE download_attempts SET status ='failed', details='%s', updated_at = '%s' WHERE id = %s"\
                    % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.downloaders[filename].attempt_id))
                    self.query("UPDATE downloads SET status = 'failed', details = '%s',updated_at='%s' WHERE remote_filename = '%s'"\
                    % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename))
                elif self.downloaders[filename].status == 'downloaded':
                    if self.downloaded_size_match(self.downloaders[filename].attempt_id):
                        self.query("UPDATE download_attempts SET status ='downloaded', details='%s', updated_at = '%s' WHERE id = %s"\
                        % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.downloaders[filename].attempt_id))
                        self.query("UPDATE downloads SET status = 'downloaded', details = '%s',updated_at='%s' WHERE remote_filename = '%s'"\
                        % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename))
                    else:
                        self.query("UPDATE download_attempts SET status ='failed', details='%s', updated_at = '%s' WHERE id = %s"\
                        % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.downloaders[filename].attempt_id))
                        self.query("UPDATE downloads SET status = 'failed', details = '%s',updated_at='%s' WHERE remote_filename = '%s'"\
                        % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename))
                del(self.downloaders[filename])
            else:
                self.query("UPDATE download_attempts SET status ='downloading', details='%s', updated_at = '%s' WHERE id = %s"\
                    % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.downloaders[filename].attempt_id))
                self.query("UPDATE downloads SET status = 'downloading', details = '%s',updated_at='%s' WHERE remote_filename = '%s'"\
                    % (self.downloaders[filename].details.replace("'","").replace('"',""),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename))
                    
    def downloaded_size_match(self,attempt_id):
        attempt_row = self.query("SELECT * FROM download_attempts WHERE id=%u" % int(attempt_id))[0]
        download = self.query("SELECT * FROM downloads WHERE id=%u" % int(attempt_row['download_id']))[0]
        
        if os.path.exists(download['filename']):
            return (os.path.getsize(download['filename']) == int(download['size']))
        else:
            dlm_cout.outs("Does not exist: %s" % download['filename'])
            return False
        
            
    def status(self):
        dls = self.query("SELECT * from downloads WHERE request_id = %s" % self.values['id'])
        print "Restore: %s" % self.guid
        print "\t\tDownloading: "
        for dl in dls:
            print "\t\t %s \t[%s]" % (dl['remote_filename'],str(dl['size']))

    def is_finished(self):
        all_downloads = self.query("SELECT * FROM downloads WHERE request_id = %s" % self.values['id'])
        finished_downloads = self.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'downloaded'" % self.values['id'])
        failed_downloads = self.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'failed'" % self.values['id'])
        downloading = self.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'downloading'" % self.values['id'])
        
        if len(downloading) > 0:
            return False
        
        if len(all_downloads) == 0 and self.downloaders == dict():
            return False
        
        if len(all_downloads) == len(finished_downloads):
            self.query("UPDATE requests SET status ='finished', updated_at='%s' WHERE id = %s"\
                    % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.values['id']))
            return True
        
        for failed_download in failed_downloads:
            number_of_attempts = len(self.query("SELECT * FROM download_attempts WHERE download_id = %s" % failed_download['id']))
            if downloader_numofretries < number_of_attempts:
                return False
        
        self.query("UPDATE requests SET status ='finished', updated_at='%s' WHERE id=%s"\
                    % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),self.values['id']) )
        return True

    def get_tries(self,filename):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        query = "SELECT * FROM restore_downloads WHERE guid = '%s' AND filename = '%s' LIMIT 1" % (self.name,filename)
        db_cur.execute(query)
        row = db_cur.fetchone()
        db_conn.close()
        return row['num_tries']
        
    def update_dl_status(self):
        db_conn = sqlite3.connect(self.db_name);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();

        for filename,dl_instance in self.downloaders.items():
            query = "UPDATE restore_downloads SET status = '%s' WHERE guid = '%s' and filename = '%s'" % (dl_instance.status.replace("'","").replace('"',''),self.name,filename)
            db_cur.execute(query)
            db_conn.commit()
        db_conn.close()

    def update_from_db(self):
        self.values = self.get_by_guid(self.guid)

    def get_by_guid(self, guid):
        result = self.query("SELECT * FROM requests WHERE guid = '%s'" % guid)
        if result == list():
            return result
        return result[0]
    
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

    def __init__(self,restore_dir ,filename,attempt_id = None):
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
        self.status = ""
        self.details = ""
        self.attempt_id = attempt_id
        self.file_dir = None
        self.start_time = 0
        self.end_time = 0
        self.total_size_got = 0
        self.file_size = 0
        
    def run(self):
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
                    self.status = 'failed'
                    self.details = 'Login failed %s' % str(self.file_name)
                self.download = True
                cwd_response = self.ftp.cwd(self.restore_dir)
                if cwd_response != "250 CWD command successful.":
                    dl_cout.outs("Restore Directory not found", OutStream.ERROR)
                    self.status = 'failed'
                    self.details = 'Directory change failed %s' % str(self.file_name)
                not_logged_in = False
            except Exception , e:
                #self.update_status({'dl_status':"Failed: '"+ self.file_name +"' -- "+ str(e)})
                #self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
                dl_cout.outs("Could not connect to host. Reason: %s. Waiting 1 sec: %s " % (self.file_name,str(e)) )
                sleep(1)
        
        try:
            self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
            self.status = 'New'
        except Exception, e:
            self.status = "failed"
            self.details = str(e)
        
        if self.status == 'failed':
            return
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
            self.finished('downloaded',str(self.total_size_got) +" bytes -- Completed in: "+\
            self.prntime(time_took))
        except Exception , e:
            self.finished('failed','Failed: in Downloader.run() %s' % str(e))
        
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
        self.status = 'downloading'
        self.details = str(self.total_size_got)+" -- "\
        + str(int( float(self.total_size_got) / float(self.file_size) * 100 )) +"% -- "\
        +str(self.speed)+" Kb/s"
        self.file.write(block)

    def finished(self,status,message):
        #print "Closing File: "+self.file_name
        dl_cout.outs(message)
        self.status = status
        self.details = message
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



