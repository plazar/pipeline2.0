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

from config import downloader_temp, rawdata_directory, downloader_api_password, \
    downloader_api_service_url, downloader_space_to_use, downloader_numofdownloads,\
    downloader_numofrestores


class DownloadModule:

    def __init__(self):
        self.LOG_FILENAME = "download_module.log"
        logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=self.LOG_FILENAME,
                    filemode='w')
        self.my_logger = logging.getLogger('MyLogger')
        self.my_logger.info('Initializing.')
        self.WebService =  Client(downloader_api_service_url).service
        self.password = downloader_api_password
        self.downloaders = []
        self.restores = []

    def run(self):
        while True:
            if self.have_space():
                if self.can_request_more():
                    response = self.restore()
                    print "Requested Restore"
                    self.my_logger.info("Requested Restore")
                    if response != "Error":
                        #self.downloaders.append(downloader())
                        print "Restore Response: "+ response
                        #self.my_logger.info("Restore Response: "+ response)
                        self.restores.append(response)
                    else:
                        self.my_logger.warning("Restore Response: "+ response)
                        
                for restore in self.restores:
                    self.getLocation(restore)

                if self.can_download_more():
                    for download in self.downloaders:
                        if download.status == 'New':
                            download.start()
                            self.my_logger.info("Starting Download: "+download.file_name)
            else:
                if not self.have_space():
                    self.my_logger.warning\
                    ("Not enough space specified in config file to store the file.")

            for download in self.downloaders[:]:
                print download.status                
                if download.status.split(':')[0] == "Downloaded" or \
                download.status.split(':')[0] == 'Failed':                    
                    self.my_logger.info(download.status)
                    self.downloaders.remove(download)
            sleep(1)
            print ""

    def getLocation(self,restore):
        self.my_logger.info("Requesting Location for: "+ restore)
        response = self.WebService.LocationTest(pw=self.password, guid=restore)
        if response != "incomplete" and response  != "Error":
#           if not self.downloading(response):
#               self.downloaders.append(downloader(response))
#               self.restores.remove(restore)

          if not self.downloading('10m'):
            self.downloaders.append(downloader('10m'))
            self.restores.remove(restore)
          else:
            self.restores.remove(restore)
    
    def downloading(self,file_name):
        for download in self.downloaders:
            if download.file_name == file_name:
                return True
        return False

    def restore(self):
        return self.WebService.RestoreTest(pw=self.password,number=30)

    def have_space(self):
        return True

    def can_download_more(self):
        downloading_count = 0
        for download in self.downloaders[:]:
                if download.status.split(':')[0] == "Downloading":
                    downloading_count += 1
        if downloading_count >= downloader_numofdownloads:
            print "Cannot downlaod more than: "+ str(downloader_numofdownloads) +" at a time."
            return False
        else:
            print "Will download."
            return True
        
    def can_request_more(self):
        if len(self.restores) >= downloader_numofrestores:
            print "Cannot have more than "+ str(downloader_numofrestores) +" at a time."
            return False
        else:
            print "Will request more files"
            return True

class downloader(Thread):

    def __init__(self,file_path):
#        self.local_drive_avail()
#        exit("end")
        Thread.__init__(self)
        self.block = {}
        self.block['size'] = 0
        self.block['time'] = 0
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.download = False
        self.ftp = None
        self.status = None
        self.file = None
        self.start_time = 0
        self.end_time = 0
        
        try:
            self.ftp = FTP('ftp.bic.mni.mcgill.ca')
        except Exception as e:
            self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
        
        if not os.path.exists(os.path.join(rawdata_directory,self.file_name)):
            try:
                self.ftp.login()
                try:
                    self.download = True
                    self.file = open(os.path.join(downloader_temp,self.file_name),'wb')
                    self.ftp.cwd(os.path.dirname(self.file_path))
                    self.status = 'New'
                except Exception as e:
                    if not self.status:
                        self.status = "Failed: '"+ self.file_name +"' -- "+ str(e)
            except Exception as e:
                if not self.status:
                    self.status = "Failed: Login failed '"+ self.file_name +"' -- "+ str(e)
        else:
            if not self.status:
                self.status = "Failed: File '"+ self.file_name +"' already exists."
        self.total_size_got = 0
        self.file_size = 0
        
    def run(self):
        if self.download:
            if self.enough_space():
                try:
                    self.start_time = time.time()
                    self.ftp.retrbinary("RETR "+self.file_path, self.write)
                    shutil.move(os.path.join(downloader_temp,self.file_name),rawdata_directory)
                    self.end_time = time.time()
                    time_took = self.end_time - self.start_time
                    self.finished("Downloaded: '"+ self.file_name +"' "\
                    +str(self.total_size_got)+" bytes -- Completed in: "+\
                    self.prntime(time_took))
                except Exception as e:
                    self.finished('Failed: '+str(e))
            else:
                self.finished('Failed: Not Enough Space to save the remote file.')
        
    def get_file_size(self):
        self.file_size = self.ftp.size(self.file_path)
        return self.file_size

    def write(self, block):
#        print len(block)
        sleep(0.01)
        self.total_size_got += len(block)
        self.speed = int(((float(self.total_size_got) - float(self.block['size'\
        ])) / float( time.time() -self.block['time'] ))/1024)
        self.block['time'] = time.time()
        self.block['size'] = self.total_size_got
        self.status = 'Downloading: '+str(self.total_size_got)+" -- "\
        + str(int(float(self.total_size_got) / float(self.file_size) * 100 )) +"% -- "\
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

controller = DownloadModule()
controller.run()